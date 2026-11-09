#!/usr/bin/env python3
"""drive_sync.py - the deterministic file layer between a working copy and Google Drive/Sheets.

Sessions reach Drive and Sheets only through MCP connector tools, which the model calls. This
tool never touches the network. It decides what a role pulls and may push, turns local files
into Sheet writes and Sheet reads back into local files, checks downloads against checksums,
and keeps the small per-working-copy state under `.drive_sync/` (gitignored).

Usage: python3 pipeline/drive_sync.py [--root DIR] <command> ...

  paths      --role ROLE [--run-id R] [--key K] [--company C | --company-file PATH]
             [--job-folder NAME] [--fill-id F]
             what the role pulls (files, globs, tabs) and may push (create, replace, tabs).
             --company-file reads the company name from a file (its UTF-8 text, surrounding
             whitespace stripped), so a name holding quotes, $(...) or backticks never passes
             through a shell command line; giving both is a usage error
  collision  --name <company>_<role>
             the three Drive lookups /apply Step 2's collision check needs
  decode     --out PATH (--tool-output FILE | --b64-file FILE) [--id ID]
             [--expect-sha256 HEX | --description "sha256=... size=..."]
             write a downloaded file, verify it, record it in .drive_sync/pulled.json
  describe   --local PATH
             the Drive description ("sha256=<hex> size=<n>") for an upload
  verify     --local PATH (--tool-output FILE | --b64-file FILE)
             compare a local file with a re-downloaded copy
  push-plan  --role ROLE [same selectors as paths]
             local files the role may push that are new or changed since they were pulled
  rows       <tab> [--from FILE] [--chunk N] [--base PATH]
             Sheet appends and updates for a local file, against the last export's snapshot.
             seen_jobs only, --base PATH: a copy of the snapshot taken when the session first
             exported the tab. A key the current snapshot holds is then updated only if the
             session changed it (its local entry differs from the base); an entry the session
             left alone keeps whatever another writer put in the Sheet since (counted in
             counts.kept_theirs). A key the base lacks is compared with the current snapshot
             as without --base; a key the current snapshot lacks is appended.
  export     <tab> --values FILE [--values FILE ...] [--out PATH] [--verify-against PATH]
             [--no-more-rows] [--discard-local]
             build the local file and the snapshot from saved get_values results. The snapshot
             describes only the Sheet, so it is written even when --verify-against finds
             differences (then `rows` writes exactly those). A ledger export refuses (exit 2,
             local file kept) while local rows with "row": null are missing from the Sheet,
             unless --discard-local; seen_jobs and tracker exports report local data they drop.
  ids        get PATH | set PATH ID
             the .drive_sync/ids.json cache of Drive folder ids

`--root DIR` (before or after the command) is the working-copy root; it defaults to the
directory that holds `pipeline/`. Relative paths resolve against it, including
`pipeline/config.yaml` and `.drive_sync/`.

Every command prints one JSON object on stdout; errors go to stderr. Exit codes: 0 ok, 1 usage
or config error, 2 data error (header mismatch, oversize cell, checksum mismatch, removed rows,
invalid ledger rows, incomplete or out-of-order pages).

Sheet encoding (one implementation, used for every tab): Sheets writes are USER_ENTERED, so
every written cell is "'" + its text (None -> "'", bools -> true/false, dicts and lists ->
canonical JSON, other scalars -> str()). The apostrophe forces plain text and is not returned
on read. `entry_json` and `detail_json` hold canonical JSON
(sort_keys=True, ensure_ascii=False, separators=(",", ":")). A cell over 50,000 characters
(apostrophe included) is a data error; nothing is ever truncated. Reads pad each row to the
header width (get_values omits trailing empty cells) and require the exact header in row 1.
In `seen_jobs`, only `key` (the row's identity) and `entry_json` are read back; the other
columns are for browsing and are ignored on export.

Company normalization (`paths --company` or `--company-file`): the Company Research Cache rule in
.claude/skills/job-application-assistant/04-job-evaluation.md - lowercase, trim, spaces to
hyphens ("Acme Corp" -> "acme-corp"), no legal-suffix normalization. No tool in the repo
implemented that rule as code, so it is implemented here exactly as written there; a name that
would not be a single file name (a path separator, a leading dot) is not pulled.
"""
from __future__ import annotations

import argparse
import base64
import binascii
import csv
import hashlib
import io
import json
import os
import re
import sys
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path

try:
    import yaml
except ImportError:  # only `paths`, `push-plan` and `ids get` need the config
    yaml = None

DEFAULT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_REL = "pipeline/config.yaml"
STATE_DIR = ".drive_sync"

SEEN_HEADER = [
    "key", "status", "rank_score", "rank_verdict", "title", "company", "location", "url",
    "first_seen", "posted_date", "rank_date", "deadline", "entry_json",
]
# Identical to the CSV header (TRACKER_HEADER in pipeline_state.py).
TRACKER_HEADER = [
    "date", "company", "sector", "role", "role_type", "channel", "status",
    "contact_person", "fit_rating", "notes", "cv_file", "cover_letter_file",
    "source", "deadline",
]
LEDGER_HEADER = ["at", "run_id", "key", "phase", "event", "status", "detail_json"]
LEDGER_FIELDS = LEDGER_HEADER[:-1]
HEADERS = {"seen_jobs": SEEN_HEADER, "tracker": TRACKER_HEADER, "ledger": LEDGER_HEADER}
TABS = tuple(HEADERS)
DEFAULT_FILES = {
    "seen_jobs": "job_scraper/seen_jobs.json",
    "tracker": "job_search_tracker.csv",
    "ledger": "pipeline/ledger.json",
}

MAX_CELL_CHARS = 50_000
CHUNK_ROWS = 25
CHUNK_CHARS = 40_000

LEDGER_PHASES = ("run", "documents", "browser")
LEDGER_EVENTS = {  # event -> the phases it may carry (the ledger vocabulary)
    "started": ("run",),
    "selected": ("documents",),
    "fired": ("documents", "browser"),
    "fire_failed": ("documents", "browser"),
    "claimed": ("documents", "browser"),
    "finished": ("documents", "browser"),
    # pipeline_state.py record: the lane's tracker_row reached the tracker (it is never appended
    # again, and stats counts it); ledger-from-runs migrates the old "recorded" flag the same way.
    "recorded": ("documents",),
    "run_finished": ("run",),
}
KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$")  # pipeline_state.py's job key pattern
RUN_ID_RE = re.compile(r"^(?:fill-)?\d{4}-\d{2}-\d{2}(?:-\d+)?$")
AT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")
SAFE_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$")
JOB_FOLDER_RE = re.compile(r"^\w[\w.-]{0,200}$")  # documents/README.md "Subfolder naming" output
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,200}$")
RANGE_RE = re.compile(
    r"^(?P<sheet>'(?:[^']|'')+'|[^'!]+)!"
    r"(?P<c1>[A-Za-z]{1,3})(?P<r1>[0-9]*)"
    r"(?::(?P<c2>[A-Za-z]{1,3})(?P<r2>[0-9]*))?$"
)

ROLES = ("orchestrator", "documents-lane", "browser-lane", "filler", "interactive")

# The data paths of the storage map: personal files kept in Drive at the same path. The
# framework .gitignore ignores every one of them; nothing outside them is ever pushed.
DATA_GLOBS = [
    "job_scraper/notion_sync.json",
    "company_research/*.json",
    "cv/main_*.tex",
    "cover_letters/cover_*.tex",
    "documents/applications/**",
    "documents/cv/**",
    "documents/linkedin/**",
    "documents/diplomas/**",
    "documents/references/**",
    "documents/projects/**",
    "documents/postings/**",
    "documents/interview/**",
    "pipeline/runs/**",
    "pipeline/fills/**",
]
BUILD_SUFFIXES = (".aux", ".log", ".out", ".pdf", ".synctex.gz", ".fls", ".fdb_latexmk", ".xdv")
# Everything else the framework .gitignore ignores that is never data: caches, dependencies,
# build folders, secrets, images, OS and editor files, backups.
IGNORED_DIRS = {".git", "__pycache__", "node_modules", ".venv", "venv", "build", ".vscode",
                ".idea", STATE_DIR}
IGNORED_NAMES = {".DS_Store", "Thumbs.db", "bun.lock", "skills-lock.json", "salary_data.json",
                 ".gitkeep", ".env"}
IGNORED_SUFFIXES = (".pyc", ".pyo", ".jpg", ".jpeg", ".png")
# Framework files that sit next to data paths (tracked by git, never pushed).
FRAMEWORK_FILES = {"cv/main_example.tex", "cover_letters/cover_example.tex"}


class SyncError(Exception):
    """A failure with its exit code; `payload` (if any) is still printed on stdout."""

    def __init__(self, code: int, message: str, payload: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.payload = payload


def usage_error(message: str) -> SyncError:
    return SyncError(1, message)


def data_error(message: str, payload: dict | None = None) -> SyncError:
    return SyncError(2, message, payload)


def warn(message: str) -> None:
    print(f"drive_sync.py: warning: {message}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Encoding, hashing, files


def canonical(obj) -> str:
    """The canonical JSON of entry_json / detail_json cells."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def text_hash(text: str) -> str:
    return sha256_hex(text.encode("utf-8"))


def cell_text(value) -> str:
    """The plain text of one cell, before the apostrophe."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list, tuple)):
        return canonical(value)
    return str(value)


def encode_cell(value) -> str:
    """What is written to the Sheet: a leading apostrophe forces plain text."""
    return "'" + cell_text(value)


def sheet_cell(value) -> str:
    """One cell as get_values returned it (display strings; anything else made a string)."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def row_hash(cells) -> str:
    return text_hash(canonical([str(c) for c in cells]))


def col_letter(number: int) -> str:
    letters = ""
    while number:
        number, rem = divmod(number - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def col_number(letters: str) -> int:
    number = 0
    for ch in letters.upper():
        number = number * 26 + (ord(ch) - 64)
    return number


def resolve(root: Path, value: str) -> Path:
    path = Path(os.path.expanduser(value))
    return path if path.is_absolute() else root / path


def rel_to_root(root: Path, path: Path) -> str | None:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return None


def shown(root: Path, path: Path) -> str:
    return rel_to_root(root, path) or str(path)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def write_json_document(path: Path, doc) -> None:
    """Exactly tools/rank_state.py save_state: indent=2, ensure_ascii=False, a trailing
    newline, atomic replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.stem}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def write_tracker_csv(path: Path, rows: list[list[str]]) -> None:
    """Header + rows, csv.writer(lineterminator="\\n"), UTF-8, atomic replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.stem}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(TRACKER_HEADER)
            writer.writerows(rows)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def write_state(path: Path, obj) -> None:
    atomic_write_bytes(
        path, (json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8"))


def read_text(path: Path, what: str) -> str:
    try:
        return path.read_text(encoding="utf-8-sig")
    except FileNotFoundError:
        raise usage_error(f"{what} not found: {path}") from None
    except IsADirectoryError:
        raise usage_error(f"{what} is a directory: {path}") from None
    except UnicodeDecodeError as exc:
        raise data_error(f"{what} is not UTF-8 text: {path} ({exc})") from None


def read_bytes(path: Path, what: str) -> bytes:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        raise usage_error(f"{what} not found: {path}") from None
    except IsADirectoryError:
        raise usage_error(f"{what} is a directory: {path}") from None


def read_json_file(path: Path, what: str):
    text = read_text(path, what)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise data_error(f"{what} is not valid JSON: {path} ({exc})") from None


# ---------------------------------------------------------------------------
# Paths and globs


def safe_relpath(value: str, allow_glob: bool = False) -> str | None:
    """A working-copy-relative POSIX path, or None (absolute, `..`, backslash, empty)."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or "\\" in text or "\0" in text or text.startswith("/") or re.match(r"^[A-Za-z]:", text):
        return None
    parts = [p for p in text.split("/") if p not in ("", ".")]
    if not parts or any(p == ".." for p in parts):
        return None
    if not allow_glob and any(ch in text for ch in "*?["):
        return None
    return "/".join(parts)


def has_wildcard(pattern: str) -> bool:
    return any(ch in pattern for ch in "*?[")


def glob_regex(pattern: str) -> re.Pattern:
    """`**` crosses folders, `*` and `?` stay inside one path component."""
    out, i = [], 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def matches_any(path: str, patterns) -> bool:
    return any(glob_regex(p).match(path) for p in patterns)


def static_prefix(pattern: str) -> str:
    parts = []
    for part in pattern.split("/"):
        if has_wildcard(part):
            break
        parts.append(part)
    return "/".join(parts)


def ignored(rel: str) -> bool:
    """Build artifacts and anything else the framework gitignores that is not data."""
    parts = rel.split("/")
    if any(p in IGNORED_DIRS for p in parts[:-1]):
        return True
    name = parts[-1]
    low = name.lower()
    return (low.endswith(BUILD_SUFFIXES) or low.endswith(IGNORED_SUFFIXES) or name in IGNORED_NAMES
            or name.startswith(".env.") or name.startswith("seen_jobs.json.bak-"))


def normalize_company(name: str) -> str:
    """04-job-evaluation.md, Company Research Cache: lowercase, trim, spaces to hyphens."""
    return name.lower().strip().replace(" ", "-")


def single_file_name(name: str) -> bool:
    return bool(name) and not name.startswith(".") and not any(ch in name for ch in "/\\\0")


# ---------------------------------------------------------------------------
# Config and .drive_sync state


def load_config(root: Path, required: bool = True) -> dict | None:
    path = root / CONFIG_REL
    if not path.is_file():
        if not required:
            return None
        raise usage_error(f"{CONFIG_REL} not found in the working copy ({root}); pull it from Drive first")
    if yaml is None:
        raise usage_error("PyYAML is required to read pipeline/config.yaml (pip install pyyaml)")
    try:
        cfg = yaml.safe_load(read_text(path, CONFIG_REL)) or {}
    except yaml.YAMLError as exc:
        raise usage_error(f"{CONFIG_REL} is not valid YAML: {exc}") from None
    if not isinstance(cfg, dict):
        raise usage_error(f"{CONFIG_REL} must be a mapping")
    drive = cfg.get("drive")
    if not isinstance(drive, dict):
        raise usage_error(f"{CONFIG_REL} has no drive: section")
    problems = []
    for name in ("root_id", "sheet_id"):
        value = drive.get(name)
        if value is not None and not (isinstance(value, str) and value.strip()):
            problems.append(f"drive.{name} must be a non-empty string")
    for name, allow_glob in (("overlay", False), ("replaceable", True)):
        items = drive.get(name)
        if items is None:
            continue
        if not isinstance(items, list):
            problems.append(f"drive.{name} must be a list")
            continue
        for item in items:
            if safe_relpath(item, allow_glob=allow_glob) is None:
                kind = "relative glob" if allow_glob else "relative path"
                problems.append(f"drive.{name} entry {item!r} is not a {kind}")
    folders = drive.get("folders")
    if folders is not None and not (isinstance(folders, dict) and all(
            isinstance(k, str) and isinstance(v, str) for k, v in folders.items())):
        problems.append("drive.folders must map folder paths to folder id strings")
    if problems:
        raise usage_error(f"{CONFIG_REL}: " + "; ".join(problems))
    return cfg


def cfg_list(cfg: dict, name: str, allow_glob: bool) -> list[str]:
    return [safe_relpath(item, allow_glob=allow_glob) for item in (cfg["drive"].get(name) or [])]


def state_path(root: Path, name: str) -> Path:
    return root / STATE_DIR / name


def load_state_json(root: Path, name: str) -> dict:
    path = state_path(root, name)
    if not path.is_file():
        return {}
    doc = read_json_file(path, f"{STATE_DIR}/{name}")
    if not isinstance(doc, dict):
        raise data_error(f"{STATE_DIR}/{name} is not a JSON object; delete it and pull again")
    return doc


def load_snapshot(root: Path, tab: str) -> dict | None:
    path = state_path(root, f"snapshot_{tab}.json")
    if not path.is_file():
        return None
    snap = read_json_file(path, f"{STATE_DIR}/snapshot_{tab}.json")
    rows = snap.get("rows") if isinstance(snap, dict) else None
    if not isinstance(rows, list) or not all(isinstance(r, dict) and isinstance(r.get("row"), int)
                                             and isinstance(r.get("hash"), str) for r in rows):
        raise data_error(f"{STATE_DIR}/snapshot_{tab}.json is corrupt; export the {tab} tab again")
    return snap


def load_base(root: Path, value: str) -> dict[str, set[str]]:
    """`rows seen_jobs --base`: an earlier copy of snapshot_seen_jobs.json -> key: entry hashes."""
    path = resolve(root, value)
    what = f"--base {shown(root, path)}"
    snap = read_json_file(path, what)
    if isinstance(snap, dict) and snap.get("tab", "seen_jobs") != "seen_jobs":
        raise usage_error(f"{what} is a snapshot of the {snap.get('tab')!r} tab, not seen_jobs")
    rows = snap.get("rows") if isinstance(snap, dict) else None
    if not isinstance(rows, list) or not all(
            isinstance(r, dict) and isinstance(r.get("row"), int) and isinstance(r.get("hash"), str)
            and isinstance(r.get("key"), str) for r in rows):
        raise data_error(f"{what} is not a seen_jobs snapshot (a copy of {STATE_DIR}/snapshot_seen_jobs.json)")
    base: dict[str, set[str]] = {}
    for r in rows:
        base.setdefault(r["key"], set()).add(r["hash"])
    return base


# ---------------------------------------------------------------------------
# Reading tool results


def find_download(obj, depth: int = 0):
    """The download_file_content result ({content: <base64>, id, mimeType, title}) inside a
    saved tool result, which may be wrapped in MCP content blocks."""
    if depth > 8:
        return None
    if isinstance(obj, dict):
        if isinstance(obj.get("content"), str):
            return obj
        for value in obj.values():
            if isinstance(value, (dict, list)):
                found = find_download(value, depth + 1)
                if found is not None:
                    return found
        if isinstance(obj.get("text"), str):
            try:
                return find_download(json.loads(obj["text"]), depth + 1)
            except ValueError:
                return None
        return None
    if isinstance(obj, list):
        for item in obj:
            found = find_download(item, depth + 1)
            if found is not None:
                return found
    return None


def b64decode(text: str, source: str, lenient: bool = False) -> bytes:
    """Strict base64 unless `lenient`: missing "=" padding is then restored. Only a caller that
    checks the bytes afterwards (an expected sha256, a comparison) may be lenient, because a
    copy that lost a character also has the wrong length, and padding it decodes to wrong
    bytes from the drop onwards."""
    data = "".join(text.split())
    if data.startswith("data:") and "," in data:
        data = data.split(",", 1)[1]
    if "-" in data or "_" in data:
        data = data.replace("-", "+").replace("_", "/")
    rem = len(data) % 4
    if rem == 1 or (rem and not lenient):
        raise data_error(f"{source}: not valid base64 (its length is not a multiple of 4: a character "
                         f"was lost or added; copy it again exactly)")
    if rem:
        data += "=" * (4 - rem)
    try:
        return base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise data_error(f"{source}: not valid base64 ({exc})") from None


def read_download(root: Path, tool_output: str | None, b64_file: str | None,
                  lenient: bool = False) -> tuple[bytes, str | None]:
    """(bytes, Drive file id or None) from a saved tool result or a bare base64 copy. `lenient`
    (missing padding restored) only when the caller checks the bytes afterwards."""
    if tool_output:
        path = resolve(root, tool_output)
        text = read_text(path, "download_file_content result")
        try:
            obj = json.loads(text)
        except ValueError:
            obj = None
        if not isinstance(obj, (dict, list)):
            return b64decode(text, str(path), lenient), None  # a saved result holding the bare base64
        found = find_download(obj)
        if found is None:
            raise data_error(f"{path}: no download_file_content result found (expected JSON with a base64 \"content\")")
        file_id = found.get("id") if isinstance(found.get("id"), str) else None
        return b64decode(found["content"], str(path), lenient), file_id
    path = resolve(root, b64_file)
    text = read_text(path, "base64 file")
    stripped = text.strip()
    if stripped.startswith(("{", "[")):  # a whole inline result was copied: accept it
        try:
            found = find_download(json.loads(stripped))
        except ValueError:
            found = None
        if found is None:
            raise data_error(f"{path}: holds JSON without a base64 \"content\"; copy just the base64")
        file_id = found.get("id") if isinstance(found.get("id"), str) else None
        return b64decode(found["content"], str(path), lenient), file_id
    return b64decode(text, str(path), lenient), None


def find_value_ranges(obj, depth: int = 0) -> list[dict]:
    """The get_values result(s) ({range, values}) inside a saved or copied tool result."""
    if depth > 8:
        return []
    if isinstance(obj, dict):
        if isinstance(obj.get("range"), str):
            return [obj]
        if isinstance(obj.get("valueRanges"), list):
            found = []
            for item in obj["valueRanges"]:
                found += find_value_ranges(item, depth + 1)
            return found
        for value in obj.values():
            if isinstance(value, (dict, list)):
                found = find_value_ranges(value, depth + 1)
                if found:
                    return found
        if isinstance(obj.get("text"), str):
            try:
                return find_value_ranges(json.loads(obj["text"]), depth + 1)
            except ValueError:
                return []
        return []
    if isinstance(obj, list):
        found = []
        for item in obj:
            found += find_value_ranges(item, depth + 1)
        return found
    return []


def parse_range(text: str) -> dict:
    """`seen_jobs!A201:M400`, `'seen_jobs'!A1:M200`, `tracker!A:N` -> sheet, columns, rows."""
    m = RANGE_RE.match(text.strip())
    if not m:
        raise data_error(f"can't parse the range {text!r} (expected something like seen_jobs!A201:M400)")
    sheet = m["sheet"]
    if sheet.startswith("'"):
        sheet = sheet[1:-1].replace("''", "'")
    start = int(m["r1"]) if m["r1"] else 1
    if m["c2"] is None:
        end = start if m["r1"] else None
        last_col = m["c1"]
    else:
        end = int(m["r2"]) if m["r2"] else None
        last_col = m["c2"]
    if start < 1 or (end is not None and end < start):
        raise data_error(f"the range {text!r} has impossible row numbers")
    return {"sheet": sheet, "first_col": col_number(m["c1"]), "last_col": col_number(last_col),
            "start": start, "end": end}


# ---------------------------------------------------------------------------
# Ledger validation


def valid_at(value) -> bool:
    if not isinstance(value, str) or not AT_RE.match(value):
        return False
    text = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


def validate_ledger(entries) -> list[str]:
    """entries: (label, row dict, detail parse error or None), in ledger order."""
    errors, selected = [], set()
    for label, item, detail_error in entries:
        problems = []
        phase, event = item.get("phase"), item.get("event")
        run_id, key = item.get("run_id"), item.get("key")
        if phase not in LEDGER_PHASES:
            problems.append(f"phase {phase!r} is not one of run, documents, browser")
        if event not in LEDGER_EVENTS:
            problems.append(f"event {event!r} is not in the ledger vocabulary")
        elif phase in LEDGER_PHASES and phase not in LEDGER_EVENTS[event]:
            problems.append(f"event {event} belongs to phase {' or '.join(LEDGER_EVENTS[event])}, not {phase}")
        if not isinstance(run_id, str) or not RUN_ID_RE.match(run_id):
            problems.append(f"run_id {run_id!r} is not YYYY-MM-DD[-n] or fill-YYYY-MM-DD[-n]")
        if phase == "run":
            if key not in ("", None):
                problems.append(f"run rows have an empty key, not {key!r}")
        elif not isinstance(key, str) or not KEY_RE.match(key):
            problems.append(f"key {key!r} does not match the job key pattern")
        if not valid_at(item.get("at")):
            problems.append(f"at {item.get('at')!r} is not an ISO date-time")
        if not isinstance(item.get("status", ""), str):
            problems.append("status is not a string")
        if detail_error:
            problems.append(detail_error)
        elif not isinstance(item.get("detail"), dict):
            problems.append("detail is not a JSON object")
        if phase in ("documents", "browser") and event != "selected" and (run_id, key) not in selected:
            problems.append(f"no selected row for ({run_id}, {key}) earlier in the ledger")
        if event == "selected":
            selected.add((run_id, key))
        if problems:
            errors.append(f"{label}: " + "; ".join(problems))
    return errors


def ledger_cells(item: dict) -> list[str]:
    """The seven cell texts of a ledger row (detail as canonical JSON)."""
    return [cell_text(item.get(name)) for name in LEDGER_FIELDS] + [canonical(item.get("detail"))]


# ---------------------------------------------------------------------------
# Local files


def load_local_seen(path: Path) -> tuple[object, dict]:
    doc = read_json_file(path, "seen_jobs file")
    seen = doc.get("seen") if isinstance(doc, dict) and "seen" in doc else doc  # legacy flat form
    if not isinstance(seen, dict):
        raise data_error(f"{path}: expected {{\"seen\": {{key: entry}}}}")
    return doc, seen


def load_local_tracker(path: Path) -> list[list[str]]:
    try:
        text = read_bytes(path, "tracker CSV").decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise data_error(f"tracker CSV is not UTF-8 text: {path} ({exc})") from None
    rows = list(csv.reader(io.StringIO(text, newline="")))
    if not rows:
        raise data_error(f"{path}: empty (no header row)")
    if rows[0] != TRACKER_HEADER:
        raise data_error(f"{path}: header mismatch: expected {TRACKER_HEADER}, got {rows[0]}")
    width, out = len(TRACKER_HEADER), []
    for number, row in enumerate(rows[1:], 2):
        if not any(row):
            continue  # a blank line, like a blank Sheet row, is not a row
        if len(row) > width:
            if any(row[width:]):
                raise data_error(f"{path}: record {number} has {len(row)} columns; the header has {width}")
            row = row[:width]
        out.append(row + [""] * (width - len(row)))
    return out


def load_local_ledger(path: Path) -> list[dict]:
    doc = read_json_file(path, "ledger file")
    rows = doc.get("rows") if isinstance(doc, dict) else None
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise data_error(f"{path}: expected {{\"rows\": [{{...}}, ...]}}")
    return rows


# ---------------------------------------------------------------------------
# Export: get_values pages -> rows -> local file + snapshot


def collect_sheet_rows(tab: str, page_files: list[Path], no_more_rows: bool = False) -> list[tuple[int, list[str]]]:
    """Data rows (row number, cells padded to the header width) after checking the pages and
    the header. Blank rows are skipped. A full last page means the tab may hold more rows,
    unless the caller says the next page lies past the end of the sheet (`no_more_rows`)."""
    header = HEADERS[tab]
    width = len(header)
    pages = []
    for path in page_files:
        found = find_value_ranges(read_json_file(path, "get_values result"))
        if not found:
            raise data_error(f"{path}: no get_values result found (expected JSON with \"range\" and \"values\")")
        pages += found
    raw_rows, expected, last = [], 1, None
    for index, page in enumerate(pages, 1):
        rng = parse_range(page["range"])
        label = f"page {index} ({page['range']})"
        if rng["sheet"] != tab:
            raise data_error(f"{label} is from tab {rng['sheet']!r}, not {tab!r}")
        if rng["first_col"] != 1:
            raise data_error(f"{label} must start at column A")
        if rng["last_col"] < width:
            raise data_error(f"{label} stops before column {col_letter(width)}; the {tab} tab has columns A:{col_letter(width)}")
        if expected is None:
            raise data_error(f"{label} follows a page with an open-ended range")
        if rng["start"] != expected:
            if index == 1:
                raise data_error(f"the first page must start at row 1 (the header row), not row {rng['start']}")
            raise data_error(f"{label} starts at row {rng['start']}; expected row {expected} (pages must be contiguous and in order)")
        values = page.get("values")
        values = [] if values is None else values
        if not isinstance(values, list) or not all(isinstance(r, list) for r in values):
            raise data_error(f"{label}: \"values\" must be a list of rows")
        if rng["end"] is not None and len(values) > rng["end"] - rng["start"] + 1:
            raise data_error(f"{label} holds {len(values)} rows, more than its range")
        for offset, raw in enumerate(values):
            raw_rows.append((rng["start"] + offset, [sheet_cell(v) for v in raw]))
        expected = rng["end"] + 1 if rng["end"] is not None else None
        last = (label, rng, len(values))
    if last is None:
        raise data_error("no pages given")
    label, rng, count = last
    if not no_more_rows and rng["end"] is not None and count == rng["end"] - rng["start"] + 1:
        size = rng["end"] - rng["start"] + 1
        raise data_error(
            f"the last page ({label}) is full, so the tab may hold more rows: fetch "
            f"{tab}!A{rng['end'] + 1}:{col_letter(width)}{rng['end'] + size} and pass it too "
            f"(if that request fails because it lies past the end of the sheet, pass --no-more-rows)")
    rows = []
    for number, cells in raw_rows:
        if len(cells) > width:
            if any(cells[width:]):
                raise data_error(f"{tab} row {number} has data beyond column {col_letter(width)}")
            cells = cells[:width]
        rows.append((number, cells + [""] * (width - len(cells))))
    if not rows or rows[0][0] != 1 or not any(rows[0][1]):
        raise data_error(
            f"the {tab} tab has no header row. For a first load into an empty tab, skip the "
            f"export and run `rows {tab}` without a snapshot (the header is appended first).")
    if rows[0][1] != header:
        raise data_error(f"{tab} header mismatch: expected {header}, got {rows[0][1]}")
    return [(number, cells) for number, cells in rows[1:] if any(cells)]


def build_seen(rows) -> tuple[dict, list[dict], list[dict]]:
    """(document, snapshot rows, duplicates). A key repeated with an identical entry_json (a
    push re-run after part of it landed, or two sessions appending the same new key) is kept
    once, at its first row; each repeat is listed in `duplicates` and stays in the snapshot
    with `duplicate_of`, so `rows` updates every copy alike and they never drift apart. A key
    repeated with a different entry_json is a conflict only a person can settle: it fails."""
    seen, snap, first, errors, duplicates = {}, [], {}, [], []
    for number, cells in rows:
        key, raw = cells[0], cells[-1]
        if not key:
            errors.append(f"row {number}: key is empty")
            continue
        if not raw:
            errors.append(f"row {number} ({key}): entry_json is empty")
            continue
        try:
            entry = json.loads(raw)
        except ValueError as exc:
            errors.append(f"row {number} ({key}): entry_json is not valid JSON ({exc})")
            continue
        if not isinstance(entry, dict):
            errors.append(f"row {number} ({key}): entry_json is not a JSON object")
            continue
        digest = text_hash(canonical(entry))
        if key in first:
            first_row, first_digest = first[key]
            if digest != first_digest:
                errors.append(f"row {number}: key {key} repeats row {first_row} with a different "
                              f"entry_json (re-read both; if the Sheet really holds two versions, make "
                              f"them equal or delete one in the Sheet)")
                continue
            duplicates.append({"key": key, "row": number, "first_row": first_row})
            snap.append({"row": number, "key": key, "hash": digest, "duplicate_of": first_row})
            continue
        first[key] = (number, digest)
        seen[key] = entry
        snap.append({"row": number, "key": key, "hash": digest})
    if errors:
        raise data_error("seen_jobs export failed; re-read these rows:\n  " + "\n  ".join(errors))
    return {"seen": seen}, snap, duplicates


def build_tracker(rows) -> tuple[list[list[str]], list[dict]]:
    return [cells for _, cells in rows], [{"row": n, "hash": row_hash(cells)} for n, cells in rows]


def build_ledger(rows) -> tuple[dict, list[dict]]:
    entries = []
    for number, cells in rows:
        detail, detail_error = None, None
        try:
            detail = json.loads(cells[6])
        except ValueError as exc:
            detail_error = f"detail_json does not parse ({exc})"
        item = {"row": number}
        item.update(zip(LEDGER_FIELDS, cells[:6]))
        item["detail"] = detail
        entries.append((f"row {number}", item, detail_error))
    errors = validate_ledger(entries)
    if errors:
        raise data_error("ledger export failed; re-read these rows from the Sheet:\n  " + "\n  ".join(errors))
    items = [item for _, item, _ in entries]
    return {"rows": items}, [{"row": i["row"], "hash": row_hash(ledger_cells(i))} for i in items]


def compare_seen(exported: dict, local_doc, snap_rows: list[dict]) -> list[dict]:
    if json.dumps(exported, sort_keys=True) == json.dumps(local_doc, sort_keys=True):
        return []
    if not (isinstance(local_doc, dict) and isinstance(local_doc.get("seen"), dict)):
        return [{"issue": "the local file is not a {\"seen\": {key: entry}} document"}]
    sheet, local = exported["seen"], local_doc["seen"]
    row_of = {r["key"]: r["row"] for r in snap_rows if "duplicate_of" not in r}
    diffs = []
    for key in sorted(set(sheet) | set(local)):
        if key not in local:
            diffs.append({"key": key, "row": row_of[key], "issue": "only in the Sheet"})
        elif key not in sheet:
            diffs.append({"key": key, "row": None, "issue": "only in the local file"})
        elif json.dumps(sheet[key], sort_keys=True) != json.dumps(local[key], sort_keys=True):
            diffs.append({"key": key, "row": row_of[key], "issue": "differs"})
    extra = sorted(set(local_doc) - {"seen"})
    if extra:
        diffs.append({"issue": f"top-level keys the Sheet does not hold: {extra}"})
    return diffs


def compare_positional(sheet_rows: list[list[str]], local_rows: list[list[str]],
                       snap_rows: list[dict]) -> list[dict]:
    diffs = []
    for i in range(max(len(sheet_rows), len(local_rows))):
        row = snap_rows[i]["row"] if i < len(snap_rows) else None
        if i >= len(local_rows):
            diffs.append({"position": i + 1, "row": row, "issue": "only in the Sheet"})
        elif i >= len(sheet_rows):
            diffs.append({"position": i + 1, "row": None, "issue": "only in the local file"})
        elif sheet_rows[i] != local_rows[i]:
            diffs.append({"position": i + 1, "row": row, "issue": "differs"})
    return diffs


def printable(text: str) -> str:
    """Text safe to print and to put in JSON output (a lone surrogate becomes \\udXXX)."""
    return text.encode("utf-8", "backslashreplace").decode("utf-8")


def few(items: list[str], limit: int = 20) -> str:
    shown_items = ", ".join(printable(str(i)) for i in items[:limit])
    return shown_items + (f" and {len(items) - limit} more" if len(items) > limit else "")


def read_existing(path: Path, loader, what: str):
    """The local file an export is about to replace, or None when it is absent or unreadable."""
    if not path.is_file():
        return None
    try:
        return loader(path)
    except SyncError as exc:
        warn(f"could not read the existing {what} {path} to check it for rows the Sheet lacks "
             f"({exc.message}); replacing it")
        return None


def unpushed_ledger_rows(local: list[dict], sheet_items: list[dict]) -> list[dict]:
    """Local rows with "row": null whose content is not in the Sheet. Sheet rows that local
    numbered rows already account for are not used to match them; each Sheet row matches one."""
    claimed = {item.get("row") for item in local if item.get("row") is not None}
    in_sheet = Counter(row_hash(ledger_cells(i)) for i in sheet_items if i["row"] not in claimed)
    missing = []
    for position, item in enumerate(local, 1):
        if item.get("row") is not None:
            continue
        try:
            digest = row_hash(ledger_cells(item))
        except UnicodeEncodeError:
            digest = None
        if digest is not None and in_sheet[digest] > 0:
            in_sheet[digest] -= 1
            continue
        missing.append({"position": position, **{name: printable(cell_text(item.get(name)))
                                                 for name in ("at", "run_id", "key", "event")}})
    return missing


def cmd_export(args, root: Path) -> dict:
    tab = args.tab
    rows = collect_sheet_rows(tab, [resolve(root, f) for f in args.values], args.no_more_rows)
    duplicates = []
    if tab == "seen_jobs":
        exported, snap_rows, duplicates = build_seen(rows)
    elif tab == "tracker":
        exported, snap_rows = build_tracker(rows)
    else:
        exported, snap_rows = build_ledger(rows)
    snapshot = {"tab": tab, "header": True, "last_row": max([1] + [n for n, _ in rows]),
                "rows": snap_rows}
    snap_path = state_path(root, f"snapshot_{tab}.json")
    result = {"tab": tab, "rows": len(rows), "snapshot": shown(root, snap_path)}
    if duplicates:
        result["duplicates"] = duplicates
        warn(f"{len(duplicates)} seen_jobs row(s) repeat an earlier row's key with the identical entry: "
             + few([f"row {d['row']} ({d['key']}) = row {d['first_row']}" for d in duplicates])
             + ". Each key is kept once; `rows seen_jobs` updates every copy. Report them; the "
               "applicant may delete the repeated rows in the Sheet (then export again).")
    if args.verify_against:
        against = resolve(root, args.verify_against)
        if tab == "seen_jobs":
            diffs = compare_seen(exported, read_json_file(against, "seen_jobs file"), snap_rows)
        elif tab == "tracker":
            diffs = compare_positional(exported, load_local_tracker(against), snap_rows)
        else:
            diffs = compare_positional([ledger_cells(i) for i in exported["rows"]],
                                       [ledger_cells(i) for i in load_local_ledger(against)], snap_rows)
        # The snapshot describes only the Sheet, so it is current whether or not the local file
        # matches. Recording it on a mismatch is what lets `rows` rewrite just the differences
        # (a re-run against the old snapshot would append rows that already landed again).
        write_state(snap_path, snapshot)
        if diffs:
            raise data_error(
                f"the {tab} tab and {shown(root, against)} differ in {len(diffs)} place(s); "
                f"{shown(root, against)} was not changed. The snapshot now records the Sheet, so "
                f"`rows {tab} --from {shown(root, against)}` writes exactly these rows (the local "
                f"file wins); rows only in the Sheet are never removed",
                {"tab": tab, "match": False, "verified_against": shown(root, against),
                 "snapshot": shown(root, snap_path), "differences": diffs})
        result.update({"match": True, "verified_against": shown(root, against)})
        return result
    out = resolve(root, args.out or DEFAULT_FILES[tab])
    if tab == "ledger":
        local = read_existing(out, load_local_ledger, "ledger file")
        unpushed = unpushed_ledger_rows(local, exported["rows"]) if local else []
        if unpushed and not args.discard_local:
            write_state(snap_path, snapshot)
            raise data_error(
                f"{shown(root, out)} holds {len(unpushed)} row(s) with \"row\": null that are not in "
                f"the Sheet (never pushed, or their append failed): "
                + few([f"local row {u['position']} ("
                       + " ".join(v for v in (u["event"], u["run_id"], u["key"]) if v) + ")"
                       for u in unpushed])
                + f". {shown(root, out)} was not changed. The snapshot now records the Sheet, so "
                  f"`rows ledger` appends exactly these (rows already in the Sheet are skipped); then "
                  f"export again. To drop them instead, pass --discard-local.",
                {"tab": tab, "rows": len(rows), "snapshot": shown(root, snap_path),
                 "path": shown(root, out), "unpushed": unpushed})
        if unpushed:
            warn(f"--discard-local: dropping {len(unpushed)} local row(s) that are not in the Sheet")
            result["discarded_local"] = len(unpushed)
    elif tab == "seen_jobs":
        local = read_existing(out, lambda path: load_local_seen(path)[1], "seen_jobs file")
        dropped = sorted(key for key in (local or {}) if key not in exported["seen"])
        if dropped:
            result["dropped_local"] = len(dropped)
            warn(f"{len(dropped)} key(s) of {shown(root, out)} are not in the Sheet and are dropped "
                 f"(never pushed, or their append failed): {few(dropped)}")
    else:
        local = read_existing(out, load_local_tracker, "tracker CSV")
        if local is not None and len(local) > len(exported):
            result["dropped_local"] = len(local) - len(exported)
            warn(f"{shown(root, out)} has {len(local) - len(exported)} row(s) past the Sheet's last row; "
                 f"they are dropped (never pushed, or their append failed)")
    if tab == "tracker":
        write_tracker_csv(out, exported)
    else:
        write_json_document(out, exported)
    write_state(snap_path, snapshot)
    result["path"] = shown(root, out)
    return result


# ---------------------------------------------------------------------------
# Rows: local file -> Sheet appends and updates


def oversize(tab: str, label: str, header: list[str], cells: list[str], errors: list[str]) -> None:
    for name, cell in zip(header, cells):
        if len(cell) > MAX_CELL_CHARS:
            errors.append(f"{tab} {label}: column {name} is {len(cell)} characters "
                          f"(limit {MAX_CELL_CHARS}, apostrophe included)")


def not_utf8(tab: str, label: str, header: list[str], cells: list[str], errors: list[str]) -> bool:
    """True, with an error recorded, when a cell holds text with no UTF-8 form (a lone surrogate
    such as \\ud83d, typically from an emoji cut in half in scraped JSON)."""
    bad = False
    for name, cell in zip(header, cells):
        try:
            cell.encode("utf-8")
        except UnicodeEncodeError:
            errors.append(f"{tab} {printable(label)}: column {name} holds text that is not valid UTF-8 "
                          f"(a lone surrogate such as \\ud83d, often half of an emoji); fix it in the local file")
            bad = True
    return bad


def chunk_appends(tab: str, rows: list[list[str]], max_rows: int) -> list[dict]:
    chunks, current, size = [], [], 0
    for row in rows:
        row_size = len(json.dumps(row, ensure_ascii=False))
        if current and (len(current) >= max_rows or size + row_size > CHUNK_CHARS):
            chunks.append(current)
            current, size = [], 0
        current.append(row)
        size += row_size
    if current:
        chunks.append(current)
    return [{"range": f"{tab}!A1", "values": chunk} for chunk in chunks]


def seen_row(key: str, entry: dict) -> list[str]:
    browse = [encode_cell(entry.get(name)) for name in SEEN_HEADER[1:-1]]
    return [encode_cell(key)] + browse + [encode_cell(canonical(entry))]


def cmd_rows(args, root: Path) -> dict:
    tab = args.tab
    header = HEADERS[tab]
    last_col = col_letter(len(header))
    source = resolve(root, args.source or DEFAULT_FILES[tab])
    snapshot = load_snapshot(root, tab)
    base = None
    if args.base is not None:
        if tab != "seen_jobs":
            raise usage_error("--base applies to the seen_jobs tab only")
        if snapshot is None:
            raise usage_error(f"--base needs the current snapshot ({STATE_DIR}/snapshot_seen_jobs.json), "
                              f"and there is none: export the seen_jobs tab again first")
        base = load_base(root, args.base)
    with_header = snapshot is None or not snapshot.get("header", True)
    new_rows, updates, errors = [], [], []
    counts = {"append": 0, "update": 0, "unchanged": 0, "missing_locally": 0}
    if base is not None:
        counts["kept_theirs"] = 0
    kept: list[str] = []

    def update(row: int, cells: list[str], key) -> None:
        updates.append({"range": f"{tab}!A{row}:{last_col}{row}", "values": [cells], "key": key, "row": row})
        counts["update"] += 1

    if tab == "seen_jobs":
        doc, seen = load_local_seen(source)
        if isinstance(doc, dict) and "seen" in doc and set(doc) - {"seen"}:
            warn(f"top-level keys other than 'seen' are not stored in the Sheet: {sorted(set(doc) - {'seen'})}")
        by_key: dict[str, list[dict]] = {}  # a key the Sheet holds twice has two rows to keep equal
        for r in snapshot["rows"] if snapshot else []:
            by_key.setdefault(r.get("key"), []).append(r)
        for key, entry in seen.items():
            # The same rules export applies, so nothing written here can lock the tab's export.
            if not key.strip():
                errors.append(f"seen_jobs key {key!r}: a key must not be empty (export refuses an "
                              f"empty key); give the entry its job key or remove it")
                continue
            if not isinstance(entry, dict):
                errors.append(f"seen_jobs key {printable(key)}: the entry is not a JSON object")
                continue
            cells = seen_row(key, entry)
            if not_utf8(tab, f"key {key}", header, cells, errors):
                continue
            oversize(tab, f"key {key}", header, cells, errors)
            previous = by_key.get(key)
            if not previous:
                new_rows.append(cells)
                counts["append"] += 1
                continue
            digest = text_hash(canonical(entry))
            stale = [r for r in previous if r["hash"] != digest]
            if base is not None and digest in base.get(key, ()):
                # The session left this entry as it found it; a difference from the Sheet now is
                # another writer's change, and theirs wins.
                if stale:
                    counts["kept_theirs"] += 1
                    kept.append(key)
                else:
                    counts["unchanged"] += 1
                continue
            for r in stale:
                update(r["row"], cells, key)
            if not stale:
                counts["unchanged"] += 1
        counts["missing_locally"] = sum(1 for key in by_key if key not in seen)
        if kept:
            warn(f"{len(kept)} seen_jobs key(s) changed in the Sheet since --base and not by this "
                 f"session; the Sheet's version is kept: {few(kept)}")
    elif tab == "tracker":
        local = load_local_tracker(source)
        snap_rows = snapshot["rows"] if snapshot else []
        if len(local) < len(snap_rows):
            raise data_error(
                f"rows were removed locally ({len(local)} local rows, {len(snap_rows)} in the Sheet "
                f"at the last export); the Sheet is never cleared")
        for i, cells in enumerate(local):
            encoded = [encode_cell(c) for c in cells]
            oversize(tab, f"row {i + 1} of {shown(root, source)}", header, encoded, errors)
            if i >= len(snap_rows):
                new_rows.append(encoded)
                counts["append"] += 1
            elif snap_rows[i]["hash"] != row_hash(cells):
                update(snap_rows[i]["row"], encoded, None)
            else:
                counts["unchanged"] += 1
    else:
        local = load_local_ledger(source)
        problems = validate_ledger([(f"local row {i + 1}", item, None) for i, item in enumerate(local)])
        if problems:
            raise data_error("ledger rows failed validation; nothing to write:\n  " + "\n  ".join(problems))
        known = {r["row"]: r["hash"] for r in snapshot["rows"]} if snapshot else None
        present, pending = set(), []
        for i, item in enumerate(local):
            number = item.get("row")
            cells = ledger_cells(item)
            label = f"local row {i + 1}"
            if not_utf8(tab, label, header, cells, errors):
                continue
            if number is None:
                pending.append((label, cells))
            elif isinstance(number, bool) or not isinstance(number, int):
                errors.append(f"ledger {label}: \"row\" must be a Sheet row number or null, not {number!r}")
            elif known is None:
                errors.append(f"ledger {label}: carries row {number} but there is no snapshot; export the ledger tab first")
            elif number not in known:
                errors.append(f"ledger {label}: row {number} was not in the Sheet at the last export")
            elif known[number] != row_hash(cells):
                errors.append(f"ledger {label}: row {number} differs from the Sheet (the ledger is append-only)")
            elif number in present:
                errors.append(f"ledger {label}: row {number} appears twice")
            else:
                present.add(number)
                counts["unchanged"] += 1
        # A null row whose content a Sheet row already holds landed in an earlier push whose
        # export did not renumber it (a failed chunk, an export refused over unpushed rows):
        # it is not appended again. Each Sheet row accounts for one local row.
        unclaimed = Counter(h for number, h in (known or {}).items() if number not in present)
        landed = 0
        for label, cells in pending:
            digest = row_hash(cells)
            if unclaimed[digest] > 0:
                unclaimed[digest] -= 1
                landed += 1
                continue
            encoded = [encode_cell(c) for c in cells]
            oversize(tab, label, header, encoded, errors)
            new_rows.append(encoded)
            counts["append"] += 1
        if landed:
            warn(f"{landed} local ledger row(s) with \"row\": null are already in the Sheet; not appended again")
        counts["unchanged"] += landed
        counts["missing_locally"] = sum(unclaimed.values())
    if errors:
        raise data_error(f"{tab}: nothing to write:\n  " + "\n  ".join(errors))
    if with_header:
        new_rows.insert(0, [encode_cell(name) for name in header])
    return {"tab": tab, "source": shown(root, source), "header": with_header,
            "append": chunk_appends(tab, new_rows, args.chunk), "update": updates, "counts": counts}


# ---------------------------------------------------------------------------
# Roles: what each pulls and may push


def lane_result_files(root: Path, rel: str, notes: list[str]) -> list[str]:
    """The data-path files a documents lane result names in `files`."""
    path = root / rel
    if not path.is_file():
        notes.append(f"{rel} is not in the working copy yet: pull it, then run paths again for the files it names")
        return []
    try:
        doc = json.loads(path.read_text(encoding="utf-8-sig"))
    except (ValueError, UnicodeDecodeError):
        notes.append(f"{rel} is not valid JSON; the files it names are unknown")
        return []
    files = doc.get("files") if isinstance(doc, dict) else None
    values = list(files.values()) if isinstance(files, dict) else files if isinstance(files, list) else []
    out = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        norm = safe_relpath(value)
        if norm is None:
            notes.append(f"files entry {value!r} skipped: not a working-copy-relative path")
        elif ignored(norm):
            continue  # PDFs are rebuilt from the .tex sources, never stored
        elif not matches_any(norm, DATA_GLOBS):
            notes.append(f"files entry {norm!r} skipped: not under the data paths")
        else:
            out.append(norm)
    return out


def role_spec(role: str, cfg: dict, args, root: Path) -> dict:
    overlay = cfg_list(cfg, "overlay", allow_glob=False)
    cfg_replace = cfg_list(cfg, "replaceable", allow_glob=True)
    files, globs, unresolved, missing = [CONFIG_REL, *overlay], [], [], []

    def sub(value, flag: str) -> str:
        if value:
            return value
        if flag not in missing:
            missing.append(flag)
        return "*"

    run_id, key, name, fill_id = args.run_id, args.key, args.job_folder, args.fill_id
    if role == "orchestrator":
        tabs = ["seen_jobs", "tracker", "ledger"]
        files.append("job_scraper/notion_sync.json")
        create = [f"pipeline/runs/{sub(run_id, '--run-id')}/**"]
        replace = ["job_scraper/notion_sync.json"]
        push_tabs = {"seen_jobs": "append+update", "tracker": "append", "ledger": "append"}
    elif role == "documents-lane":
        tabs = ["ledger"]
        if args.company:
            normalized = normalize_company(args.company)
            if single_file_name(normalized):
                files.append(f"company_research/{normalized}.json")
            else:
                unresolved.append(f"company_research/<company>.json not pulled: {args.company!r} "
                                  f"does not normalize to a single file name")
        else:
            unresolved.append("company_research/<company>.json not pulled: needs --company")
        n, r, k = sub(name, "--job-folder"), sub(run_id, "--run-id"), sub(key, "--key")
        create = [f"cv/main_{n}.tex", f"cover_letters/cover_{n}.tex",
                  f"documents/applications/{n}/job_posting.md",
                  f"documents/applications/{n}/application_form_answers.md",
                  f"pipeline/runs/{r}/lanes/{k}.json", f"pipeline/runs/{r}/lanes/{k}.md"]
        replace = ["company_research/*.json"]
        push_tabs = {"ledger": "append"}
    elif role == "browser-lane":
        tabs = ["ledger"]
        if run_id and key:
            lane = f"pipeline/runs/{run_id}/lanes/{key}.json"
            files.append(lane)
            files += lane_result_files(root, lane, unresolved)
        else:
            unresolved.append("pipeline/runs/<run>/lanes/<key>.json not pulled: needs --run-id and --key")
        if name:
            files += [f"cv/main_{name}.tex", f"cover_letters/cover_{name}.tex",
                      f"documents/applications/{name}/application_form_answers.md"]
        r, k = sub(run_id, "--run-id"), sub(key, "--key")
        create = [f"pipeline/runs/{r}/lanes/{k}.browser.json", f"pipeline/runs/{r}/lanes/{k}.browser.md"]
        replace = []
        push_tabs = {"ledger": "append"}
    elif role == "filler":
        tabs = ["ledger", "tracker"]  # fill-queue holds jobs the tracker shows as already applied
        create = [f"pipeline/fills/{sub(fill_id, '--fill-id')}/**"]
        replace = []
        push_tabs = {"ledger": "append"}
    else:  # interactive: the applicant's own machine
        tabs = list(TABS)
        files.append("job_scraper/notion_sync.json")
        globs = [g for g in DATA_GLOBS if g not in files]
        create = list(DATA_GLOBS)
        replace = list(cfg_replace)
        push_tabs = {"seen_jobs": "append+update", "tracker": "append+update", "ledger": "append"}
    # Only what drive.replaceable also covers may be replaced.
    replace = [g for g in replace if g in cfg_replace or matches_any(g, cfg_replace)]
    for flag in missing:
        unresolved.append(f"{flag} not given: push globs use * in its place")
    deduped = list(dict.fromkeys(files))
    return {"role": role,
            "pull": {"files": deduped, "globs": globs, "tabs": tabs},
            "push": {"create": create, "replace": replace, "tabs": push_tabs},
            "unresolved": unresolved}


def read_company_file(root: Path, value: str) -> str:
    """--company-file: the company name is the file's UTF-8 text, surrounding whitespace stripped."""
    path = resolve(root, value)
    data = read_bytes(path, "--company-file")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise usage_error(f"--company-file {path} is not UTF-8 text ({exc})") from None
    return text.strip()


def check_selectors(args, root: Path) -> None:
    if getattr(args, "company_file", None) is not None:
        if args.company is not None:
            raise usage_error("give --company or --company-file, not both")
        args.company = read_company_file(root, args.company_file)
        if not args.company:
            raise usage_error(f"--company-file {args.company_file} is empty")
    if args.run_id is not None and (not SAFE_COMPONENT_RE.match(args.run_id) or ".." in args.run_id):
        raise usage_error(f"--run-id {args.run_id!r} is not a run id")
    if args.fill_id is not None and (not SAFE_COMPONENT_RE.match(args.fill_id) or ".." in args.fill_id):
        raise usage_error(f"--fill-id {args.fill_id!r} is not a fill id")
    if args.key is not None and not KEY_RE.match(args.key):
        raise usage_error(f"--key {args.key!r} does not match the job key pattern")
    if args.job_folder is not None and (not JOB_FOLDER_RE.match(args.job_folder) or ".." in args.job_folder):
        raise usage_error(f"--job-folder {args.job_folder!r} is not a <company>_<role> folder name")
    if args.company is not None and not args.company.strip():
        raise usage_error("--company is empty")


def cmd_paths(args, root: Path) -> dict:
    check_selectors(args, root)
    return role_spec(args.role, load_config(root), args, root)


def local_candidates(root: Path, patterns: list[str]) -> dict[str, str]:
    """Working-copy files matching the globs: path -> "file" or "symlink"."""
    regexes = [glob_regex(p) for p in patterns]
    found = {}
    for pattern in patterns:
        if not has_wildcard(pattern):
            path = root / pattern
            if path.is_symlink():
                found[pattern] = "symlink"
            elif path.is_file():
                found[pattern] = "file"
            continue
        base = root / static_prefix(pattern)
        if not base.is_dir() or base.is_symlink():
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = sorted(d for d in dirnames
                                 if d not in IGNORED_DIRS and not os.path.islink(os.path.join(dirpath, d)))
            for filename in filenames:
                full = Path(dirpath) / filename
                rel = full.relative_to(root).as_posix()
                if any(rx.match(rel) for rx in regexes):
                    found[rel] = "symlink" if full.is_symlink() else "file"
    return found


def cmd_push_plan(args, root: Path) -> dict:
    check_selectors(args, root)
    cfg = load_config(root)
    spec = role_spec(args.role, cfg, args, root)
    replace_rx = [glob_regex(g) for g in spec["push"]["replace"]]
    cfg_replace_rx = [glob_regex(g) for g in cfg_list(cfg, "replaceable", allow_glob=True)]
    pulled = load_state_json(root, "pulled.json")
    candidates = local_candidates(root, spec["push"]["create"] + spec["push"]["replace"])
    files, refused = [], []
    for rel in sorted(candidates):
        previous = pulled.get(rel)
        if ignored(rel) or (rel in FRAMEWORK_FILES and not isinstance(previous, dict)):
            continue
        if candidates[rel] == "symlink":
            refused.append({"path": rel, "reason": "a symlink; never pushed"})
            continue
        data = (root / rel).read_bytes()
        try:
            data.decode("utf-8")
        except UnicodeDecodeError:
            refused.append({"path": rel, "reason": "not UTF-8 text; binary files are never pushed by sessions (upload by hand)"})
            continue
        sha, size = sha256_hex(data), len(data)
        if isinstance(previous, dict):
            if previous.get("sha256") == sha:
                continue  # unchanged since it was pulled
            if not (any(rx.match(rel) for rx in replace_rx) and any(rx.match(rel) for rx in cfg_replace_rx)):
                refused.append({"path": rel, "reason": "create-only: changed since it was pulled, and not replaceable"})
                continue
            action, replaces_id = "replace", previous.get("id")
        else:
            action, replaces_id = "create", None
        folder, _, title = rel.rpartition("/")
        files.append({"path": rel, "folder": folder, "title": title, "action": action, "sha256": sha,
                      "size": size, "description": f"sha256={sha} size={size}", "replaces_id": replaces_id})
    for rel, previous in sorted(pulled.items()):
        path = root / rel
        if rel in candidates or not isinstance(previous, dict) or path.is_symlink() or not path.is_file():
            continue
        if sha256_hex(path.read_bytes()) != previous.get("sha256"):
            refused.append({"path": rel, "reason": f"create-only: changed since it was pulled, and role {args.role} does not push it"})
    return {"role": args.role, "files": files, "refused": refused, "unresolved": spec["unresolved"]}


# ---------------------------------------------------------------------------
# Files: decode, describe, verify


def expected_checksum(expect: str | None, description: str | None) -> tuple[str | None, int | None]:
    if expect is not None:
        value = expect.strip().lower()
        if not SHA256_RE.match(value):
            raise usage_error("--expect-sha256 must be 64 hex characters")
        return value, None
    if description:
        sha = re.search(r"sha256=([0-9a-fA-F]{64})(?![0-9a-fA-F])", description)
        if not sha:
            return None, None
        size = re.search(r"\bsize=(\d+)\b", description)
        return sha.group(1).lower(), int(size.group(1)) if size else None
    return None, None


def first_differing_line(a: bytes, b: bytes) -> int | None:
    try:
        text_a, text_b = a.decode("utf-8"), b.decode("utf-8")
    except UnicodeDecodeError:
        return None

    def lines(text: str) -> list[str]:
        parts = text.split("\n")
        return [p + "\n" for p in parts[:-1]] + ([parts[-1]] if parts[-1] else [])

    la, lb = lines(text_a), lines(text_b)
    for number, (x, y) in enumerate(zip(la, lb), 1):
        if x != y:
            return number
    return min(len(la), len(lb)) + 1


def cmd_decode(args, root: Path) -> dict:
    expected_sha, expected_size = expected_checksum(args.expect_sha256, args.description)
    if args.id is not None and not DRIVE_ID_RE.match(args.id):
        raise usage_error(f"--id {args.id!r} is not a Drive file id")
    data, tool_id = read_download(root, args.tool_output, args.b64_file, lenient=expected_sha is not None)
    if args.id and tool_id and args.id != tool_id:
        raise data_error(f"the download is of file id {tool_id}, not {args.id}; nothing written")
    out = resolve(root, args.out)
    sha, size = sha256_hex(data), len(data)
    verified = None
    if expected_sha is not None:
        if sha != expected_sha or (expected_size is not None and size != expected_size):
            raise data_error(
                f"checksum mismatch for {shown(root, out)}: got sha256={sha} size={size}, expected "
                f"sha256={expected_sha} size={expected_size}; nothing written",
                {"path": shown(root, out), "sha256": sha, "size": size, "verified": False,
                 "expected_sha256": expected_sha, "expected_size": expected_size})
        verified = True
    else:
        why = ("the description carries no sha256=..." if args.description
               else "no --expect-sha256 or --description was given")
        warn(f"{why}, so {shown(root, out)} is not verified against a checksum")
    atomic_write_bytes(out, data)
    rel = rel_to_root(root, out)
    if rel is None:
        warn(f"{out} is outside the working copy; not recorded in {STATE_DIR}/pulled.json")
    else:
        pulled = load_state_json(root, "pulled.json")
        pulled[rel] = {"id": args.id or tool_id, "sha256": sha, "size": size}
        write_state(state_path(root, "pulled.json"), pulled)
    return {"path": rel or str(out), "sha256": sha, "size": size, "verified": verified}


def cmd_describe(args, root: Path) -> dict:
    path = resolve(root, args.local)
    data = read_bytes(path, "local file")
    sha, size = sha256_hex(data), len(data)
    return {"path": shown(root, path), "description": f"sha256={sha} size={size}", "sha256": sha, "size": size}


def cmd_verify(args, root: Path) -> dict:
    path = resolve(root, args.local)
    local = read_bytes(path, "local file")
    remote, remote_id = read_download(root, args.tool_output, args.b64_file, lenient=True)
    result = {"path": shown(root, path), "match": local == remote,
              "local": {"sha256": sha256_hex(local), "size": len(local)},
              "remote": {"sha256": sha256_hex(remote), "size": len(remote), "id": remote_id},
              "first_diff_line": None}
    if not result["match"]:
        result["first_diff_line"] = first_differing_line(local, remote)
        where = f" (first difference on line {result['first_diff_line']})" if result["first_diff_line"] else ""
        raise data_error(
            f"{result['path']}: the Drive copy differs from the local file: local sha256="
            f"{result['local']['sha256']} size={len(local)}, Drive sha256={result['remote']['sha256']} "
            f"size={len(remote)}{where}", result)
    return result


# ---------------------------------------------------------------------------
# collision, ids


def cmd_collision(args, root: Path) -> dict:
    name = args.name.strip()
    if not JOB_FOLDER_RE.match(name) or ".." in name:
        raise usage_error(f"--name {args.name!r} is not a <company>_<role> name")
    return {"name": name, "lookups": [
        {"folder": "cv", "title": f"main_{name}.tex", "kind": "file"},
        {"folder": "cover_letters", "title": f"cover_{name}.tex", "kind": "file"},
        {"folder": "documents/applications", "title": name, "kind": "folder"},
    ]}


def drive_path(value: str) -> str:
    """A Drive folder path as a repo-relative POSIX path ("" is the data root)."""
    text = value.strip()
    if text in ("", ".", "./", "/"):
        return ""
    norm = safe_relpath(text.rstrip("/"))
    if norm is None:
        raise usage_error(f"{value!r} is not a repo-relative folder path")
    return norm


def cmd_ids(args, root: Path) -> dict:
    path = drive_path(args.path)
    cache = load_state_json(root, "ids.json")
    if args.ids_command == "set":
        if not DRIVE_ID_RE.match(args.id):
            raise usage_error(f"{args.id!r} is not a Drive id")
        cache[path] = args.id
        write_state(state_path(root, "ids.json"), cache)
        return {"path": path, "id": args.id}
    if isinstance(cache.get(path), str):
        return {"path": path, "id": cache[path], "source": "cache"}
    cfg = load_config(root, required=False)
    drive = cfg["drive"] if cfg else {}
    if path == "" and drive.get("root_id"):
        return {"path": path, "id": drive["root_id"], "source": "config"}
    folders = drive.get("folders") or {}
    for key, value in folders.items():
        if drive_path(key) == path:
            return {"path": path, "id": value, "source": "config"}
    return {"path": path, "id": None, "source": None}


# ---------------------------------------------------------------------------
# CLI


class Parser(argparse.ArgumentParser):
    """Usage errors exit 1 (argparse's default is 2, which here means a data error)."""

    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(1, f"{self.prog}: error: {message}\n")


def positive_int(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not a whole number") from None
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return value


def build_parser() -> Parser:
    root_help = "working-copy root (default: the directory that holds pipeline/)"
    parser = Parser(prog="drive_sync.py", description="The file layer between a working copy and Google Drive/Sheets.")
    parser.add_argument("--root", default=None, help=root_help)
    common = Parser(add_help=False)
    common.add_argument("--root", default=argparse.SUPPRESS, help=root_help)
    sub = parser.add_subparsers(dest="command", required=True, metavar="<command>")

    def selectors(sp):
        sp.add_argument("--role", required=True, choices=ROLES)
        sp.add_argument("--run-id")
        sp.add_argument("--key")
        company = sp.add_mutually_exclusive_group()
        company.add_argument("--company")
        company.add_argument("--company-file", help="a file holding the company name (UTF-8; "
                                                    "surrounding whitespace stripped)")
        sp.add_argument("--job-folder")
        sp.add_argument("--fill-id")

    def download_source(sp):
        group = sp.add_mutually_exclusive_group(required=True)
        group.add_argument("--tool-output", help="a saved download_file_content result (JSON with content)")
        group.add_argument("--b64-file", help="bare base64 copied from an inline result (whitespace ignored)")

    sp = sub.add_parser("paths", parents=[common], help="what a role pulls and may push")
    selectors(sp)
    sp = sub.add_parser("collision", parents=[common], help="the Drive lookups of /apply's collision check")
    sp.add_argument("--name", required=True, help="<company>_<role>")
    sp = sub.add_parser("decode", parents=[common], help="write a downloaded file and record it as pulled")
    sp.add_argument("--out", required=True)
    download_source(sp)
    sp.add_argument("--id", help="the Drive file id")
    expected = sp.add_mutually_exclusive_group()
    expected.add_argument("--expect-sha256")
    expected.add_argument("--description", help='the Drive description, "sha256=<hex> size=<n>"')
    sp = sub.add_parser("describe", parents=[common], help="the Drive description for an upload")
    sp.add_argument("--local", required=True)
    sp = sub.add_parser("verify", parents=[common], help="compare a local file with a re-downloaded copy")
    sp.add_argument("--local", required=True)
    download_source(sp)
    sp = sub.add_parser("push-plan", parents=[common], help="files a role may push that are new or changed")
    selectors(sp)
    sp = sub.add_parser("rows", parents=[common], help="Sheet appends and updates for a local file")
    sp.add_argument("tab", choices=TABS)
    sp.add_argument("--from", dest="source", help="the local file (default: the tab's file)")
    sp.add_argument("--chunk", type=positive_int, default=CHUNK_ROWS, help="rows per append chunk")
    sp.add_argument("--base", help="seen_jobs: a copy of the snapshot from the session's first export; "
                                   "entries the session did not change keep the Sheet's version")
    sp = sub.add_parser("export", parents=[common], help="build the local file from get_values pages")
    sp.add_argument("tab", choices=TABS)
    sp.add_argument("--values", action="append", required=True, help="a saved get_values result (repeat, in order)")
    sp.add_argument("--out")
    sp.add_argument("--verify-against")
    sp.add_argument("--discard-local", action="store_true",
                    help="ledger: replace pipeline/ledger.json even though rows with \"row\": null are not in the Sheet")
    sp.add_argument("--no-more-rows", action="store_true",
                    help="the page after the last one lies past the end of the sheet, so a full last page is complete")
    sp = sub.add_parser("ids", parents=[common], help="the .drive_sync/ids.json folder id cache")
    ids = sp.add_subparsers(dest="ids_command", required=True, metavar="get|set")
    get = ids.add_parser("get", parents=[common])
    get.add_argument("path")
    put = ids.add_parser("set", parents=[common])
    put.add_argument("path")
    put.add_argument("id")
    return parser


COMMANDS = {
    "paths": cmd_paths,
    "collision": cmd_collision,
    "decode": cmd_decode,
    "describe": cmd_describe,
    "verify": cmd_verify,
    "push-plan": cmd_push_plan,
    "rows": cmd_rows,
    "export": cmd_export,
    "ids": cmd_ids,
}


def emit(obj: dict) -> None:
    text = json.dumps(obj, ensure_ascii=False, indent=2)
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:  # a lone surrogate: \\uXXXX escapes are still the same JSON
        text = json.dumps(obj, ensure_ascii=True, indent=2)
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    for stream, errors in ((sys.stdout, "strict"), (sys.stderr, "backslashreplace")):
        try:
            stream.reconfigure(encoding="utf-8", errors=errors)
        except (AttributeError, ValueError):
            pass
    args = build_parser().parse_args(argv)
    root = Path(os.path.expanduser(args.root)).resolve() if args.root else DEFAULT_ROOT
    if not root.is_dir():
        print(f"drive_sync.py: error: --root {root} is not a directory", file=sys.stderr)
        return 1
    try:
        result = COMMANDS[args.command](args, root)
    except SyncError as exc:
        if exc.payload is not None:
            emit(exc.payload)
        print(f"drive_sync.py: error: {exc.message}", file=sys.stderr)
        return exc.code
    except UnicodeError as exc:  # text with no UTF-8 form that no check above caught
        print(f"drive_sync.py: error: text that is not valid UTF-8 (a lone surrogate such as "
              f"\\ud83d, often half of an emoji): {exc}; nothing written", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"drive_sync.py: error: {exc}", file=sys.stderr)
        return 1
    emit(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
