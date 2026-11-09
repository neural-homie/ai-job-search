#!/usr/bin/env python3
"""State helper for the nightly job-application pipeline.

The orchestrator (pipeline/skills/nightly-pipeline), the lanes
(pipeline/skills/documents-lane, pipeline/skills/browser-lane) and the morning
filler (pipeline/skills/morning-filler) call this instead of reading or editing
the working copy's state files by hand. job_scraper/seen_jobs.json and the
tracker are read here and never printed in full, the same rule /rank follows.

History lives in pipeline/ledger.json, the local copy of the Sheet's ledger tab
(exported with drive_sync.py). Commands that change state append rows to it
with "row": null and print them under "ledger_rows"; the session pushes them
with `drive_sync.py rows ledger`. Rows are ordered by Sheet row number; rows
not pushed yet ("row": null) come after every numbered row, in file order.

Each selected job runs in two phases, each in its own stand-alone session:
  documents  /apply and the form answer key (started by the nightly run)
  browser    filling the application in Chrome (started by the morning filler,
             one at a time by default, because every session shares Chrome)
Lane results are also written to pipeline/runs/<run>/lanes/<key>.json and
<key>.browser.json, the run's human-readable record.

Subcommands (run from the working-copy root, or pass --root):
  check             validate pipeline/config.yaml and the stage files it names
  new-run           start a nightly run: print its id and append "started"
  check-run         after pushing "started": does this session own the earliest
                    active run? (--nonce from new-run / fill-start)
  select            pick tonight's jobs from the ranked backlog
  lanes             documents lane progress, and which lanes to start next
  mark-fired        record that a lane session was started (or failed to start)
  write-lane        (lanes) claim a lane, or store its result JSON (stdin)
  check-claim       (lanes) after pushing the claim: does this session own it?
  record            add "drafted" tracker rows for finished documents lanes and
                    append a "recorded" row for each (append-tracker is an alias)
  finish-run        append "run_finished" for a nightly run
  fill-start        start a morning fill run: print its id and append "started"
  fill-queue        drafted jobs ready for a browser lane, and which to start now
  fill-finish       append "run_finished" for a fill run
  stats             pick quality: how many pipeline drafts went on to be submitted
  ledger-from-runs  one-time migration: ledger rows from old pipeline/runs folders

Global options (before or after the subcommand): --root DIR (default: the
folder above pipeline/) and --now ISO-8601 (default: the real time in the
configured time zone).

Exit codes: 0 ok, 1 error, 2 usage, 3 duplicate claim, 4 browser phase not
ready, 5 another run is active (or this run is not the earliest active one).

Nothing here touches the network, Drive or the Sheet.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import secrets
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

DEFAULT_ROOT = Path(__file__).resolve().parent.parent
ROOT = PIPE = RUNS = FILLS = CONFIG = LEDGER = SEEN = TRACKER = DEFAULT_ROOT  # set_root() sets these
NOW_OVERRIDE: datetime | None = None


def set_root(root: Path) -> None:
    global ROOT, PIPE, RUNS, FILLS, CONFIG, LEDGER, SEEN, TRACKER
    ROOT = Path(root).resolve()
    PIPE = ROOT / "pipeline"
    RUNS = PIPE / "runs"
    FILLS = PIPE / "fills"
    CONFIG = PIPE / "config.yaml"
    LEDGER = PIPE / "ledger.json"
    SEEN = ROOT / "job_scraper" / "seen_jobs.json"
    TRACKER = ROOT / "job_search_tracker.csv"


set_root(DEFAULT_ROOT)

TRACKER_HEADER = [
    "date", "company", "sector", "role", "role_type", "channel", "status",
    "contact_person", "fit_rating", "notes", "cv_file", "cover_letter_file",
    "source", "deadline",
]
# Tracker status vocabulary from /outcome (legacy space spellings accepted on read).
FINAL_STATUSES = {"hired", "rejected", "no_response", "no response",
                  "offer_declined", "offer declined", "withdrawn"}
SUBMITTED_STATUSES = {"applied", "interview", "offer", "hired", "rejected",
                      "no_response", "no response", "offer_declined", "offer declined"}

PHASES = ("documents", "browser")
DOC_STATUSES = {"running", "drafted", "skipped", "needs_review", "error"}
BROWSER_STATUSES = {"running", "filled", "drafted", "error"}
DOC_FINAL = DOC_STATUSES - {"running"}
BROWSER_FINAL = BROWSER_STATUSES - {"running"}
DOC_KEYS = {"status", "stage_reached", "company", "role", "folder", "apply_score", "apply_verdict",
            "application_page", "files", "left_for_you", "please_check", "questions", "reason",
            "notes", "tracker_row"}
BROWSER_KEYS = {"status", "application_page", "tab_id", "browser_tab", "filled", "left_for_you",
                "please_check", "questions", "reason", "notes"}

KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
RUN_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(-\d+)?$")
FILL_RE = re.compile(r"^fill-\d{4}-\d{2}-\d{2}(-\d+)?$")
BUILTIN_STAGES = {"record", "report"}

# Ledger vocabulary (DESIGN-core.md). EVENTS is also the tie-break order for
# rows that share a timestamp in ledger-from-runs.
LEDGER_FIELDS = ("row", "at", "run_id", "key", "phase", "event", "status", "detail")
EVENTS = ("started", "selected", "fired", "fire_failed", "claimed", "finished", "recorded", "run_finished")
EVENT_RANK = {name: i for i, name in enumerate(EVENTS)}
LANE_EVENTS = ("selected", "fired", "fire_failed", "claimed", "finished", "recorded")
SELECTED_FIELDS = ("url", "title", "company", "rank_score", "rank_verdict", "attempt", "browser")
SHEET_CELL_LIMIT = 50_000  # characters per Sheet cell, counting the leading apostrophe
# What a migrated lane result keeps when the whole of it would not fit in one Sheet cell.
CORE_RESULT_KEYS = ("status", "stage_reached", "company", "role", "folder", "apply_score", "apply_verdict",
                    "application_page", "files", "reason", "tracker_row", "key", "pipeline_run", "phase",
                    "updated_at", "tab_id", "browser_tab")

SELECTION_DEFAULTS = {
    "max_applications": 3,
    "min_rank_score": 60,
    "verdicts": [],
    "max_per_company": 1,
    "skip_location_flag": True,
    "skip_language_flag": True,
    "skip_guard_flagged": True,
    "max_rank_age_days": 21,
    "max_posting_age_days": 45,
    "exclude_companies": [],
    "include_title_keywords": [],
    "exclude_title_keywords": [],
    "retry_errored_lanes": True,
    "max_lane_attempts": 2,
}
SELECTION_NUMBERS = ("max_applications", "min_rank_score", "max_per_company", "max_rank_age_days",
                     "max_posting_age_days", "max_lane_attempts")
LANE_DEFAULTS = {"max_parallel": 3, "max_parallel_browser": 1, "first_check_minutes": 60,
                 "check_every_minutes": 20, "browser_check_minutes": 10, "max_wait_minutes": 360,
                 "max_fill_age_days": 7, "browser_max_minutes": 45}
LANE_NUMBERS = tuple(LANE_DEFAULTS)
TASK_ID_KEYS = ("documents_task_id", "browser_task_id", "task_id")
# Placeholder values from pipeline/config.example.yaml that a real config must replace.
PLACEHOLDERS = {"TASK_ID", "DRIVE_FOLDER_ID", "SPREADSHEET_ID", "CHROME_DEVICE_ID"}


# ---------------------------------------------------------------- helpers

def fail(msg: str, code: int = 1, **extra) -> None:
    print(json.dumps({"error": msg, **extra}, ensure_ascii=False), file=sys.stderr)
    sys.exit(code)


def emit(obj) -> None:
    print(json.dumps(obj, indent=1, ensure_ascii=False))


def rel(path: Path) -> str:
    try:
        return Path(path).resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def is_whole(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def load_config(path: Path | None = None) -> dict:
    path = path or CONFIG
    try:
        import yaml  # PyYAML; the repo's own tools/lint_skills.py needs it too
    except ImportError:
        fail("pipeline_state.py needs PyYAML to read pipeline/config.yaml (pip install pyyaml)")
    if not path.exists():
        fail(f"{rel(path)} not found")
    try:
        with path.open(encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        fail(f"{rel(path)} is not valid YAML: {exc}")
    if not isinstance(cfg, dict):
        fail(f"{rel(path)}: the top level must be a mapping")
    return cfg


def section(cfg: dict, name: str) -> dict:
    value = cfg.get(name)
    return value if isinstance(value, dict) else {}


def require_numbers(settings: dict, names, where: str, minimum: int) -> None:
    for name in names:
        if not is_whole(settings.get(name)) or settings[name] < minimum:
            fail(f"{where}.{name} must be a whole number >= {minimum}; run pipeline_state.py check")


def selection_settings(cfg: dict, strict: bool = True) -> dict:
    settings = dict(SELECTION_DEFAULTS)
    settings.update({k: v for k, v in section(cfg, "selection").items() if v is not None})
    if strict:
        require_numbers(settings, SELECTION_NUMBERS, "selection", 0)
    return settings


def lane_settings(cfg: dict, strict: bool = True) -> dict:
    settings = dict(LANE_DEFAULTS)
    settings.update({k: v for k, v in section(cfg, "lanes").items() if v is not None})
    if not settings.get("documents_task_id") and settings.get("task_id"):
        settings["documents_task_id"] = settings["task_id"]  # old name, kept as an alias
    if strict:
        require_numbers(settings, LANE_NUMBERS, "lanes", 1)
    return settings


def stage_phase(cfg: dict, name: str) -> str:
    stage = section(cfg, "stage_defs").get(name)
    return "browser" if isinstance(stage, dict) and stage.get("phase") == "browser" else "documents"


def stage_enabled(cfg: dict, name: str) -> bool:
    stage = section(cfg, "stage_defs").get(name)
    return not isinstance(stage, dict) or stage.get("enabled", True) is not False


def lane_stages(cfg: dict, phase: str) -> list[str]:
    names = section(cfg, "stages").get("lane") or []
    if not isinstance(names, list):
        return []
    return [n for n in names if stage_phase(cfg, n) == phase and stage_enabled(cfg, n)]


def config_tz(cfg: dict):
    name = cfg.get("timezone") or "America/Los_Angeles"
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(str(name))
    except Exception:  # zoneinfo or tzdata missing: fall back to UTC
        return timezone.utc


def local_now(cfg: dict) -> datetime:
    tz = config_tz(cfg)
    if NOW_OVERRIDE is not None:
        moment = NOW_OVERRIDE if NOW_OVERRIDE.tzinfo else NOW_OVERRIDE.replace(tzinfo=tz)
        return moment.astimezone(tz)
    return datetime.now(tz)


def stamp(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds")


def parse_date(value) -> date | None:
    """Defensive: anything that isn't YYYY-MM-DD is treated as absent."""
    if not isinstance(value, str) or not re.match(r"^\d{4}-\d{2}-\d{2}", value):
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def parse_iso(value) -> datetime | None:
    """An ISO-8601 time as written (naive stays naive); a trailing Z means UTC."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text[-1] in "Zz":
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def parse_time(value) -> datetime | None:
    moment = parse_iso(value)
    if moment is None:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def minutes_between(earlier: datetime, later: datetime) -> int:
    return int((later - earlier).total_seconds() // 60)


def norm_text(value) -> str:
    return " ".join(re.sub(r"[^0-9a-z]+", " ", str(value or "").lower()).split())


def norm_company(value) -> str:
    return re.sub(r"[^0-9a-z]+", "", str(value or "").lower())


def norm_url(value) -> str:
    try:
        parts = urlsplit(str(value or "").strip())
    except ValueError:
        return ""
    host = (parts.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return f"{host}{parts.path.rstrip('/')}" if host else ""


def same_company(a: str, b: str) -> bool:
    a, b = norm_company(a), norm_company(b)
    if not a or not b:
        return False
    return a == b or (min(len(a), len(b)) >= 4 and (a in b or b in a))


def company_excluded(company: str, exclusion: str) -> bool:
    """Short names like "K&R" match as a whole word or phrase ("K&R Services, Inc.")."""
    if same_company(company, exclusion):
        return True
    phrase = str(exclusion or "").strip().lower()
    if not phrase:
        return False
    pattern = r"(?<![0-9a-z])" + re.escape(phrase) + r"(?![0-9a-z])"
    return re.search(pattern, str(company or "").lower()) is not None


def relative_path_problem(value) -> str | None:
    """None when value is a working-copy-relative path, else what is wrong with it."""
    if not isinstance(value, str) or not value.strip():
        return "must be a non-empty string"
    text = value.strip().replace("\\", "/")
    if text.startswith(("/", "~")) or re.match(r"^[A-Za-z]:", text) or "://" in text:
        return "must be a working-copy-relative path, not an absolute one"
    if ".." in PurePosixPath(text).parts:
        return "must stay inside the working copy (no '..')"
    return None


def read_json(path: Path):
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1, ensure_ascii=False)
        fh.write("\n")


def write_json_atomic(path: Path, data) -> None:
    """Atomic replace: a half-written ledger.json would lose the run's history."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=1, ensure_ascii=False)
            fh.write("\n")
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def check_key(key: str) -> str:
    if not KEY_RE.match(key or ""):
        fail(f"not a job key: {key!r}")
    return key


def run_kind(run_id: str) -> str | None:
    if RUN_RE.match(run_id or ""):
        return "nightly"
    if FILL_RE.match(run_id or ""):
        return "fill"
    return None


def check_run_id(run_id: str, kind: str = "nightly") -> str:
    if run_kind(run_id) != kind:
        fail(f"not a {'run' if kind == 'nightly' else 'fill'} id: {run_id!r}")
    return run_id


def valid_date(value: str) -> bool:
    return bool(DATE_RE.match(value or "")) and parse_date(value) is not None


def run_sort_key(run_id: str) -> tuple:
    """2026-11-08 < 2026-11-08-2 < 2026-11-08-10 (numeric suffix, not string order)."""
    m = re.match(r"^(?:fill-)?(\d{4}-\d{2}-\d{2})(?:-(\d+))?$", run_id or "")
    return (m.group(1), int(m.group(2) or 1)) if m else (str(run_id), 0)


def result_path(run_id: str, key: str, phase: str) -> Path:
    suffix = ".json" if phase == "documents" else ".browser.json"
    return RUNS / run_id / "lanes" / f"{key}{suffix}"


def last(rows: list):
    return rows[-1] if rows else None


# ---------------------------------------------------------------- ledger

class Ledger:
    """pipeline/ledger.json, with its rows in ledger order."""

    def __init__(self, doc: dict, rows: list[dict], path: Path):
        self.doc = doc          # the file as read; existing rows are written back untouched
        self.path = path
        self.rows: list[dict] = []
        self._runs: dict[str, list[dict]] = {}
        self._lanes: dict[tuple[str, str, str], list[dict]] = {}
        self._selected: dict[str, dict[str, dict]] = {}
        for row in rows:
            self._add(row)

    def _add(self, row: dict) -> None:
        self.rows.append(row)
        if row["phase"] == "run":
            self._runs.setdefault(row["run_id"], []).append(row)
            return
        self._lanes.setdefault((row["run_id"], row["key"], row["phase"]), []).append(row)
        if row["event"] == "selected":
            self._selected.setdefault(row["run_id"], {}).setdefault(row["key"], row)

    def run_ids(self) -> set[str]:
        return {row["run_id"] for row in self.rows}

    def started(self, run_id: str) -> dict | None:
        return next((r for r in self._runs.get(run_id, []) if r["event"] == "started"), None)

    def starts(self, run_id: str) -> list[dict]:
        """Every started row for this id, in ledger order. More than one means two sessions
        exported the ledger before either pushed and both took the same id; the first owns it."""
        return [r for r in self._runs.get(run_id, []) if r["event"] == "started"]

    def finished_run(self, run_id: str) -> dict | None:
        return next((r for r in self._runs.get(run_id, []) if r["event"] == "run_finished"), None)

    def selected(self, run_id: str) -> dict[str, dict]:
        """key -> the job's first selected row in this run, in pick order."""
        return dict(self._selected.get(run_id, {}))

    def selection_runs(self) -> list[str]:
        """Nightly runs with picks, in the order of their first selected row."""
        return [rid for rid in self._selected if RUN_RE.match(rid)]

    def lane_rows(self, run_id: str, key: str, phase: str) -> list[dict]:
        return list(self._lanes.get((run_id, key, phase), []))

    def lane(self, run_id: str, key: str, phase: str) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {name: [] for name in LANE_EVENTS}
        for row in self._lanes.get((run_id, key, phase), []):
            if row["event"] in out:
                out[row["event"]].append(row)
        return out

    def lane_keys(self):
        return list(self._lanes)

    def append(self, new_rows: list[dict]) -> list[dict]:
        if not new_rows:
            return new_rows
        self.doc.setdefault("rows", []).extend(new_rows)
        write_json_atomic(self.path, self.doc)
        for row in new_rows:  # unpushed rows sort after everything already there
            self._add(row)
        return new_rows


def ledger_row_problem(row) -> str | None:
    if not isinstance(row, dict):
        return "not an object"
    number = row.get("row")
    if number is not None and not is_whole(number):
        return '"row" must be a sheet row number or null'
    for name in ("at", "run_id", "phase", "event"):
        if not isinstance(row.get(name), str) or not row.get(name):
            return f'"{name}" must be a non-empty string'
    for name in ("key", "status"):
        if row.get(name) is not None and not isinstance(row.get(name), str):
            return f'"{name}" must be a string'
    if row.get("detail") is not None and not isinstance(row.get("detail"), dict):
        return '"detail" must be an object'
    return None


def ledger_order(index: int, row: dict) -> tuple:
    """Sheet row number first; rows not pushed yet (row null) after all of them, in file order."""
    number = row.get("row")
    return (0, number, index) if is_whole(number) else (1, 0, index)


def read_ledger() -> tuple[Ledger | None, str | None]:
    """(ledger, None), or (None, why it can't be read). A missing file is (None, None)."""
    if not LEDGER.exists():
        return None, None
    try:
        doc = read_json(LEDGER)
    except (OSError, ValueError) as exc:
        return None, f"not valid JSON: {exc}"
    if not isinstance(doc, dict) or not isinstance(doc.get("rows"), list):
        return None, 'must hold {"rows": [...]}'
    ordered = []
    for i, raw in enumerate(doc["rows"]):
        problem = ledger_row_problem(raw)
        if problem:
            return None, f"rows[{i}]: {problem}"
        row = {name: raw.get(name) for name in LEDGER_FIELDS}
        row["key"] = row["key"] or ""
        row["status"] = row["status"] or ""
        row["detail"] = row["detail"] or {}
        ordered.append((ledger_order(i, raw), row))
    ordered.sort(key=lambda pair: pair[0])
    return Ledger(doc, [row for _, row in ordered], LEDGER), None


def load_ledger(required: bool = True) -> Ledger:
    ledger, problem = read_ledger()
    if problem:
        fail(f"pipeline/ledger.json {problem}")
    if ledger is None:
        if required:
            fail("pipeline/ledger.json not found: export the ledger tab first (drive_sync.py export ledger ...)")
        ledger = Ledger({"rows": []}, [], LEDGER)
    return ledger


def make_row(now: datetime, run_id: str, key: str, phase: str, event: str, status: str = "",
             detail: dict | None = None) -> dict:
    return {"row": None, "at": stamp(now), "run_id": run_id, "key": key, "phase": phase,
            "event": event, "status": status, "detail": detail if detail is not None else {}}


def active_runs(ledger: Ledger, kind: str, now: datetime, max_wait: int) -> list[str]:
    """Runs of this kind whose first started row is less than max_wait minutes old and that
    have no run_finished, in ledger order of those started rows."""
    finished = {r["run_id"] for r in ledger.rows if r["phase"] == "run" and r["event"] == "run_finished"}
    considered: set[str] = set()
    active: list[str] = []
    for row in ledger.rows:
        rid = row["run_id"]
        if row["phase"] != "run" or row["event"] != "started" or run_kind(rid) != kind or rid in considered:
            continue
        considered.add(rid)
        moment = parse_time(row["at"])
        if rid not in finished and moment is not None and minutes_between(moment, now) < max_wait:
            active.append(rid)
    return active


def new_run_id(ledger: Ledger, base: str, folder: Path) -> str:
    taken = ledger.run_ids()
    run_id, n = base, 1
    while run_id in taken or (folder / run_id).exists():
        n += 1
        run_id = f"{base}-{n}"
    return run_id


def require_job(ledger: Ledger, run_id: str, key: str) -> dict:
    row = ledger.selected(run_id).get(key)
    if row is None:
        fail(f"{key} is not one of run {run_id}'s jobs (the ledger has no selected row for it)")
    return row


def job_message(run_id: str, key: str, detail: dict, phase: str) -> dict:
    return {"pipeline_run": run_id, "phase": phase, "key": key, "url": detail.get("url"),
            "title": detail.get("title"), "company": detail.get("company"),
            "rank_score": detail.get("rank_score"), "rank_verdict": detail.get("rank_verdict"),
            "attempt": detail.get("attempt", 1)}


def doc_state(lane: dict[str, list[dict]]) -> str:
    fin = last(lane["finished"])
    if fin and fin["status"] in DOC_FINAL:
        return fin["status"]
    if lane["fired"] or lane["claimed"]:
        return "running"
    if lane["fire_failed"]:
        return "fire_failed"
    return "queued"


def browser_state(detail: dict, documents: str, lane: dict[str, list[dict]]) -> str:
    if not detail.get("browser"):
        return "off"
    if documents != "drafted":
        return "n/a"
    fin = last(lane["finished"])
    if fin and fin["status"] in BROWSER_FINAL:
        return fin["status"]
    if lane["fired"] or lane["claimed"]:
        return "running"
    if lane["fire_failed"]:
        return "fire_failed"
    return "ready"


def last_fire_failure(rows: list[dict]) -> dict | None:
    """The fire_failed row when the lane's latest fire event is a failure (a later fire clears it)."""
    failure = None
    for row in rows:
        if row["event"] == "fired":
            failure = None
        elif row["event"] == "fire_failed":
            failure = row
    return failure


def prior_attempts(ledger: Ledger, exclude_run: str) -> dict[str, list[dict]]:
    """key -> [{run, status, fired_at}] for documents lanes started on other nightly runs.

    A lane counts as started when the ledger has a fired, claimed or finished row
    for it. status is the final documents status, or "unfinished" when a lane was
    started but never left a final result. Jobs picked but never started are not
    attempts, so they stay eligible.
    """
    attempts: dict[tuple[str, str], dict] = {}
    for rid, key, phase in ledger.lane_keys():
        if phase != "documents" or rid == exclude_run or not RUN_RE.match(rid) or not key:
            continue
        lane = ledger.lane(rid, key, phase)
        if not (lane["fired"] or lane["claimed"] or lane["finished"]):
            continue
        fin = last(lane["finished"])
        status = fin["status"] if fin and fin["status"] in DOC_FINAL else "unfinished"
        starts = [parse_time(r["at"]) for r in (lane["fired"] or lane["claimed"])]
        starts = [t for t in starts if t is not None]
        attempts[(rid, key)] = {"run": rid, "status": status, "fired_at": max(starts) if starts else None}
    seen: dict[str, list[dict]] = {}
    for rid, key in sorted(attempts, key=lambda pair: run_sort_key(pair[0])):
        seen.setdefault(key, []).append(attempts[(rid, key)])
    return seen


# ---------------------------------------------------------------- tracker

def read_tracker() -> tuple[list[dict], list[str]]:
    if not TRACKER.exists():
        return [], list(TRACKER_HEADER)
    with TRACKER.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        header = list(reader.fieldnames or TRACKER_HEADER)
    return rows, header


def tracker_match(entry: dict, rows: list[dict]) -> tuple[dict | None, str]:
    url = norm_url(entry.get("url"))
    title = norm_text(entry.get("title"))
    for row in rows:
        if url and norm_url(row.get("source")) == url:
            return row, "same posting URL"
        if title and norm_text(row.get("role")) == title and same_company(entry.get("company"), row.get("company")):
            return row, "same company and role"
    return None, ""


def ensure_tracker_header() -> list[str]:
    if not TRACKER.exists():
        TRACKER.write_text(",".join(TRACKER_HEADER) + "\n", encoding="utf-8")
        return list(TRACKER_HEADER)
    raw = TRACKER.read_text(encoding="utf-8")
    first, sep, rest = raw.partition("\n")
    header = next(csv.reader([first]))
    if "deadline" not in header:
        # Same one-line header fix /apply Step 6b and /outcome make; no data row changes.
        TRACKER.write_text(first.rstrip("\r") + ",deadline" + (sep or "\n") + rest, encoding="utf-8")
        header.append("deadline")
    return header


def append_tracker_row(row: dict, header: list[str]) -> None:
    raw = TRACKER.read_bytes()
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerow([str(row.get(h, "") or "") for h in header])
    with TRACKER.open("a", encoding="utf-8", newline="") as fh:
        if raw and not raw.endswith(b"\n"):
            fh.write("\n")
        fh.write(buf.getvalue())


# ---------------------------------------------------------------- check

def check_drive(cfg: dict, problems: list[str]) -> dict | None:
    drive = cfg.get("drive")
    if drive is None:
        problems.append("drive: section is missing (root_id, sheet_id, overlay, replaceable)")
        return None
    if not isinstance(drive, dict):
        problems.append("drive must be a mapping")
        return None
    for name in ("root_id", "sheet_id"):
        value = drive.get(name)
        if not isinstance(value, str) or not value.strip():
            problems.append(f"drive.{name} must be a non-empty string")
        elif value.strip() in PLACEHOLDERS:
            problems.append(f"drive.{name} is still the placeholder {value.strip()}")
    folders = drive.get("folders")
    if folders is not None:
        if not isinstance(folders, dict):
            problems.append("drive.folders must map repo-relative folder paths to Drive folder ids")
        else:
            for path, folder_id in folders.items():
                why = relative_path_problem(path)
                if why:
                    problems.append(f"drive.folders key {path!r} {why}")
                if not isinstance(folder_id, str) or not folder_id.strip():
                    problems.append(f"drive.folders[{path!r}] must be a Drive folder id")
    for name, what in (("overlay", "path"), ("replaceable", "glob")):
        value = drive.get(name)
        if not isinstance(value, list):
            problems.append(f"drive.{name} must be a list of relative {what}s")
            continue
        for item in value:
            why = relative_path_problem(item)
            if why:
                problems.append(f"drive.{name} entry {item!r} {why}")
    return {"overlay": drive.get("overlay"), "replaceable": drive.get("replaceable"),
            "folders_cached": len(folders) if isinstance(folders, dict) else 0}


def cmd_check(args) -> None:
    cfg = load_config()
    problems: list[str] = []
    warnings: list[str] = []
    stages = cfg.get("stages") or {}
    defs = cfg.get("stage_defs") or {}
    if not isinstance(stages, dict):
        problems.append("stages must be a mapping")
        stages = {}
    if not isinstance(defs, dict):
        problems.append("stage_defs must be a mapping")
        defs = {}
    for group in ("before_lanes", "lane", "after_lanes"):
        names = stages.get(group)
        if not isinstance(names, list):
            problems.append(f"stages.{group} must be a list")
            continue
        for name in names:
            stage = defs.get(name)
            if not isinstance(stage, dict):
                problems.append(f"stage '{name}' (in stages.{group}) has no entry under stage_defs")
                continue
            run = str(stage.get("run") or "")
            if not run:
                problems.append(f"stage_defs.{name}.run is empty")
            elif run == "builtin":
                if name not in BUILTIN_STAGES:
                    problems.append(f"stage_defs.{name}: only {sorted(BUILTIN_STAGES)} are builtin")
            elif run.endswith(".md") and not (ROOT / run).is_file():
                problems.append(f"stage_defs.{name}.run points at a missing file: {run}")
            if stage.get("phase") not in (None, "documents", "browser"):
                problems.append(f"stage_defs.{name}.phase must be documents or browser")
            if stage.get("phase") == "browser" and group != "lane":
                problems.append(f"stage '{name}' is a browser stage, so it belongs in stages.lane")
    lane_names = stages.get("lane") if isinstance(stages.get("lane"), list) else []
    if "autofill" in lane_names and "form-answers" in lane_names and lane_names.index("autofill") < lane_names.index("form-answers"):
        problems.append("stages.lane: autofill needs the answers file, so form-answers must come first")

    sel = selection_settings(cfg, strict=False)
    for name in SELECTION_NUMBERS:
        if not is_whole(sel.get(name)) or sel[name] < 0:
            problems.append(f"selection.{name} must be a whole number >= 0")

    raw_lanes = section(cfg, "lanes")
    lanes = lane_settings(cfg, strict=False)
    if raw_lanes.get("task_id"):
        if raw_lanes.get("documents_task_id"):
            warnings.append("lanes.task_id is ignored because lanes.documents_task_id is set; remove it")
        else:
            warnings.append("lanes.task_id is the old name: rename it to lanes.documents_task_id")
    browser_on = bool(lane_stages(cfg, "browser"))
    for name, task, needed in (("documents_task_id", "Job documents lane", True),
                               ("browser_task_id", "Job browser lane", browser_on)):
        value = lanes.get(name)
        if not needed and not value:
            continue
        if not isinstance(value, str) or not value.strip():
            problems.append(f"lanes.{name} is not set to the \"{task}\" scheduled task's id")
        elif value.strip() in PLACEHOLDERS:
            problems.append(f"lanes.{name} is still the placeholder {value.strip()}")
    docs_id, browser_id = lanes.get("documents_task_id"), lanes.get("browser_task_id")
    if isinstance(docs_id, str) and docs_id.strip() and docs_id == browser_id:
        problems.append("lanes.documents_task_id and lanes.browser_task_id must name two different tasks")
    for name in LANE_NUMBERS:
        if not is_whole(lanes.get(name)) or lanes[name] < 1:
            problems.append(f"lanes.{name} must be a whole number >= 1")

    autofill = defs.get("autofill") if isinstance(defs.get("autofill"), dict) else {}
    if "autofill" in lane_names and stage_enabled(cfg, "autofill") and autofill.get("browser", "chrome") == "chrome":
        device = autofill.get("chrome_device_id")
        if not isinstance(device, str) or not device.strip() or device.strip() in PLACEHOLDERS:
            warnings.append("stage_defs.autofill.chrome_device_id is not set: browser lanes and the morning "
                            "filler can't pick the Chrome extension")

    drive = check_drive(cfg, problems)
    ledger, problem = read_ledger()
    if problem:
        problems.append(f"pipeline/ledger.json {problem}")
    ledger_info = None
    if ledger is not None:
        ledger_info = {"rows": len(ledger.rows),
                       "unpushed": sum(1 for r in ledger.rows if r["row"] is None)}
    emit({"ok": not problems, "problems": problems, "warnings": warnings, "selection": sel,
          "lanes": {k: v for k, v in lanes.items() if k not in TASK_ID_KEYS},
          "drive": drive, "ledger": ledger_info,
          "stages": {g: stages.get(g) for g in ("before_lanes", "lane", "after_lanes")},
          "documents_stages": lane_stages(cfg, "documents"), "browser_stages": lane_stages(cfg, "browser")})
    sys.exit(0 if not problems else 1)


# ---------------------------------------------------------------- new-run / check-run / finish-run

def cmd_new_run(args) -> None:
    cfg = load_config()
    lanes = lane_settings(cfg)
    now = local_now(cfg)
    if args.max_applications is not None and args.max_applications < 0:
        fail("--max-applications must be a whole number >= 0")
    base = args.date or now.date().isoformat()
    if not valid_date(base):
        fail(f"--date must be YYYY-MM-DD, got {base!r}")
    ledger = load_ledger()
    active = active_runs(ledger, "nightly", now, lanes["max_wait_minutes"])
    if active:
        fail(f"run {active[0]} started less than {lanes['max_wait_minutes']} minutes ago and has no "
             "run_finished row, so another run is not started", code=5, active_runs=active)
    run_id = new_run_id(ledger, base, RUNS)
    path = RUNS / run_id
    (path / "lanes").mkdir(parents=True, exist_ok=True)
    (path / "stages").mkdir(exist_ok=True)
    # The nonce tells this session's start apart from a concurrent one that took the same id
    # (both exported the ledger before either pushed); check-run and finish-run take it.
    detail = {"test": bool(args.test), "max_applications": args.max_applications,
              "nonce": secrets.token_hex(16)}
    write_json(path / "run.json", {"run_id": run_id, "started_at": stamp(now),
                                   "timezone": cfg.get("timezone"), **detail})
    row = make_row(now, run_id, "", "run", "started", "", detail)
    ledger.append([row])
    emit({"run_id": run_id, "dir": rel(path), **detail, "ledger_rows": [row]})


def cmd_check_run(args) -> None:
    cfg = load_config()
    lanes = lane_settings(cfg)
    now = local_now(cfg)
    kind = run_kind(args.run_id)
    if kind is None:
        fail(f"not a run id: {args.run_id!r}")
    ledger = load_ledger()
    starts = ledger.starts(args.run_id)
    if not starts:
        fail(f"run {args.run_id} has no started row in the ledger: push the ledger and export it again first")
    active = active_runs(ledger, kind, now, lanes["max_wait_minutes"])
    earliest = bool(active) and active[0] == args.run_id
    owner = starts[0]["detail"].get("nonce")
    if args.nonce is not None:
        owns = bool(args.nonce) and owner == args.nonce
    else:
        # Without a nonce two sessions that share the id can't be told apart, so neither goes on.
        owns = len(starts) == 1
    ok = earliest and owns
    if ok and starts[0]["row"] is None:
        # Until the Sheet has this start, a concurrent session can't see it and would pass too.
        fail(f"run {args.run_id}'s started row is not in the Sheet yet: push the ledger and export it again first")
    out = {"run_id": args.run_id, "ok": ok, "earliest_active": active[0] if active else None,
           "active_runs": active, "starts": len(starts), "owns_run_id": owns}
    if not ok:
        out["note"] = ("another session took this run id first: stop (finish-run with --nonce appends nothing "
                       "for a shared id)" if not owns else
                       "an earlier run is still active: this start is a duplicate")
    emit(out)
    sys.exit(0 if ok else 5)


def finish(run_id: str, aborted: str | None, id_name: str, nonce: str | None = None) -> None:
    cfg = load_config()
    now = local_now(cfg)
    ledger = load_ledger()
    first = ledger.started(run_id)
    if first is None:
        fail(f"{run_id} has no started row in the ledger")
    if nonce is not None and first["detail"].get("nonce") != nonce:
        # This session's start shared the id with another session's earlier start: the run
        # belongs to that session, so finishing it here would end the other session's run.
        emit({id_name: run_id, "status": None, "owns_run_id": False,
              "note": "another session owns this run id; nothing appended", "ledger_rows": []})
        return
    done = ledger.finished_run(run_id)
    if done is not None:
        emit({id_name: run_id, "status": done["status"], "note": "already finished; nothing appended",
              "ledger_rows": []})
        return
    status = "aborted" if aborted is not None else "done"
    row = make_row(now, run_id, "", "run", "run_finished", status, {"reason": aborted})
    ledger.append([row])
    emit({id_name: run_id, "status": status, "ledger_rows": [row]})


def cmd_finish_run(args) -> None:
    finish(check_run_id(args.run_id, "nightly"), args.aborted, "run_id", args.nonce)


# ---------------------------------------------------------------- select

def cmd_select(args) -> None:
    cfg = load_config()
    sel = selection_settings(cfg)
    lanes = lane_settings(cfg)
    now = local_now(cfg)
    today = parse_date(args.today) if args.today else now.date()
    if args.today and today is None:
        fail("--today must be YYYY-MM-DD")
    run_id = args.run_id or ""
    if run_id:
        check_run_id(run_id, "nightly")
    ledger = load_ledger()
    started = ledger.started(run_id) if run_id else None
    manifest_path = RUNS / run_id / "manifest.json"

    if not args.dry_run:
        if started is None:
            fail(f"run {run_id} has no started row in the ledger: run new-run first")
        done = ledger.finished_run(run_id)
        if done is not None:
            fail(f"run {run_id} already finished ({done['status']}); start a new run")
        picked = ledger.selected(run_id)
        if picked and args.force:
            fail("this run's picks are already in the ledger, which keeps every pick; to choose again, "
                 f"finish it (finish-run --run-id {run_id} --aborted REASON) and start a new run")
        if picked or (manifest_path.exists() and not args.force):
            counts = {}
            try:
                counts = read_json(manifest_path).get("counts", {}) if manifest_path.exists() else {}
            except (OSError, ValueError, AttributeError):
                counts = {}
            emit({"run_id": run_id, "note": "already selected for this run",
                  "selected": [{"key": key, **{k: row["detail"].get(k) for k in SELECTED_FIELDS}}
                               for key, row in picked.items()],
                  "counts": counts})
            return

    # A test run's cap (new-run --max-applications) can only lower tonight's pick.
    run_cap = (started or {}).get("detail", {}).get("max_applications")
    if is_whole(run_cap) and 0 <= run_cap < sel["max_applications"]:
        sel["max_applications"] = run_cap

    if not SEEN.exists():
        fail("job_scraper/seen_jobs.json not found: run /scrape first")
    seen = read_json(SEEN).get("seen", {})
    rows, _ = read_tracker()
    prior = prior_attempts(ledger, exclude_run=run_id)

    verdicts = {norm_text(v) for v in sel["verdicts"] or []}
    excl_companies = [c for c in sel["exclude_companies"] or [] if str(c).strip()]
    incl_words = [norm_text(w) for w in sel["include_title_keywords"] or [] if norm_text(w)]
    excl_words = [norm_text(w) for w in sel["exclude_title_keywords"] or [] if norm_text(w)]

    counts = {"ranked": 0, "below_floor": 0, "eligible": 0, "selected": 0}
    eligible, skipped = [], []

    def skip(key, entry, reason):
        skipped.append({"key": key, "title": entry.get("title"), "company": entry.get("company"),
                        "url": entry.get("url"), "rank_score": entry.get("rank_score"),
                        "reason": reason})

    for key, entry in seen.items():
        if entry.get("status") != "ranked":
            continue
        counts["ranked"] += 1
        score = entry.get("rank_score")
        if not isinstance(score, (int, float)) or score < sel["min_rank_score"]:
            counts["below_floor"] += 1
            continue
        verdict = entry.get("rank_verdict") or ""
        loc = entry.get("location_verdict")
        if loc is None and entry.get("location") in ("PASS", "FAIL", "FLAG"):
            loc = entry.get("location")  # legacy field, see /rank Step 4
        lang = entry.get("language_gate")
        guard = entry.get("guard")  # absent means the integrity gate never ran
        deadline = parse_date(entry.get("deadline"))
        rank_date = parse_date(entry.get("rank_date"))
        posted = parse_date(entry.get("posted_date"))
        title_n = norm_text(entry.get("title"))

        if not KEY_RE.match(key):
            skip(key, entry, "job key has characters the pipeline can't use in a file name"); continue
        if verdicts and norm_text(verdict) not in verdicts:
            skip(key, entry, f"verdict {verdict or 'unknown'} not in selection.verdicts"); continue
        if loc == "FAIL":
            skip(key, entry, "location deal-breaker"); continue
        if loc == "FLAG" and sel["skip_location_flag"]:
            skip(key, entry, "location flagged by /rank: your call"); continue
        if lang == "FAIL":
            skip(key, entry, "language deal-breaker"); continue
        if lang == "FLAG" and sel["skip_language_flag"]:
            skip(key, entry, "language flagged by /rank: your call"); continue
        if sel["skip_guard_flagged"] and not guard:
            skip(key, entry, "no posting integrity verdict recorded: re-rank it or read the posting first"); continue
        if sel["skip_guard_flagged"] and guard != "clean":
            skip(key, entry, "posting integrity flag: read the posting first"); continue
        if deadline and deadline < today:
            skip(key, entry, f"deadline passed ({deadline.isoformat()})"); continue
        if sel["max_rank_age_days"] and rank_date and (today - rank_date).days > sel["max_rank_age_days"]:
            skip(key, entry, f"ranked {rank_date.isoformat()}, older than {sel['max_rank_age_days']} days"); continue
        if sel["max_posting_age_days"] and posted and (today - posted).days > sel["max_posting_age_days"]:
            skip(key, entry, f"posted {posted.isoformat()}, older than {sel['max_posting_age_days']} days"); continue
        if any(company_excluded(entry.get("company"), c) for c in excl_companies):
            skip(key, entry, "company excluded in config"); continue
        if incl_words and not any(w in title_n for w in incl_words):
            skip(key, entry, "title has none of include_title_keywords"); continue
        hit = next((w for w in excl_words if w in title_n), None)
        if hit:
            skip(key, entry, f"title contains excluded keyword '{hit}'"); continue
        row, why = tracker_match(entry, rows)
        if row is not None:
            skip(key, entry, f"already in the tracker ({why}, status {row.get('status') or 'unknown'})"); continue
        history = prior.get(key, [])
        running = next((h for h in history if h["status"] == "unfinished" and h["fired_at"]
                        and minutes_between(h["fired_at"], now) < lanes["max_wait_minutes"]), None)
        if running:
            skip(key, entry, f"still running in run {running['run']}"); continue
        if history:
            last_try = history[-1]
            retry = (sel["retry_errored_lanes"] and last_try["status"] in ("error", "unfinished")
                     and len(history) < sel["max_lane_attempts"])
            if not retry:
                skip(key, entry, f"already handled on run {last_try['run']} ({last_try['status']})"); continue
        eligible.append((key, entry, score, deadline, posted))

    counts["eligible"] = len(eligible)
    eligible.sort(key=lambda t: (-t[2], t[3] or date.max, -(t[4].toordinal() if t[4] else 0)))

    browser_on = bool(lane_stages(cfg, "browser")) and not args.no_browser
    selected, picked_companies = [], []
    for key, entry, score, deadline, posted in eligible:
        company = entry.get("company") or key
        if len(selected) >= sel["max_applications"]:
            skip(key, entry, "over tonight's max_applications; eligible next run"); continue
        # "Acme" and "Acme Inc" count as one company for the per-night cap.
        if sum(1 for c in picked_companies if same_company(c, company)) >= sel["max_per_company"]:
            skip(key, entry, "max_per_company reached tonight; eligible next run"); continue
        picked_companies.append(company)
        selected.append({
            "key": key, "title": entry.get("title"), "company": entry.get("company"),
            "url": entry.get("url"), "rank_score": score, "rank_verdict": entry.get("rank_verdict"),
            "deadline": deadline.isoformat() if deadline else None,
            "posted_date": posted.isoformat() if posted else None, "portal": entry.get("portal"),
            "attempt": len(prior.get(key, [])) + 1, "browser": browser_on,
        })
    counts["selected"] = len(selected)

    out = {"run_id": run_id, "today": today.isoformat(), "browser_enabled": browser_on,
           "max_applications": sel["max_applications"],
           "selected": [{k: j[k] for k in ("key", "title", "company", "url", "rank_score", "rank_verdict",
                                            "deadline", "attempt")} for j in selected],
           "skipped": skipped, "counts": counts}
    if args.dry_run:
        out["note"] = "dry run: nothing written"
        emit(out)
        return
    manifest = {"run_id": run_id, "created_at": stamp(now),
                "browser_enabled": browser_on,
                "browser_off_reason": args.no_browser or None,
                "documents_stages": lane_stages(cfg, "documents"), "browser_stages": lane_stages(cfg, "browser"),
                "selection_settings": sel, "run_max_applications": run_cap if is_whole(run_cap) else None,
                "jobs": selected, "skipped": skipped, "counts": counts}
    write_json(manifest_path, manifest)
    new_rows = [make_row(now, run_id, job["key"], "documents", "selected", "",
                         {k: job[k] for k in SELECTED_FIELDS}) for job in selected]
    ledger.append(new_rows)
    out["ledger_rows"] = new_rows
    emit(out)


# ---------------------------------------------------------------- lanes

def cmd_lanes(args) -> None:
    cfg = load_config()
    lanes = lane_settings(cfg)
    now = local_now(cfg)
    run_id = check_run_id(args.run_id, "nightly")
    ledger = load_ledger()
    if ledger.started(run_id) is None:
        fail(f"run {run_id} has no started row in the ledger")
    jobs, fired = [], []
    running = 0
    queued: list[tuple[str, dict]] = []
    picked = ledger.selected(run_id)
    for key, sel_row in picked.items():
        detail = sel_row["detail"]
        doc = ledger.lane(run_id, key, "documents")
        state = doc_state(doc)
        blane = ledger.lane(run_id, key, "browser")
        bstate = browser_state(detail, state, blane)
        running += state == "running"
        if state == "queued":
            queued.append((key, detail))
        fired.extend(t for t in (parse_time(r["at"]) for r in doc["fired"]) if t is not None)
        reason = None
        if bstate not in ("off", "n/a", "ready"):
            reason = (last(blane["finished"]) or {}).get("detail", {}).get("reason")
        reason = reason or (last(doc["finished"]) or {}).get("detail", {}).get("reason")
        jobs.append({"key": key, "company": detail.get("company"), "title": detail.get("title"),
                     "documents": state, "browser": bstate, "reason": reason})
    start = queued[:max(0, lanes["max_parallel"] - running)]
    out = {"run_id": run_id, "browser_enabled": any(r["detail"].get("browser") for r in picked.values()),
           "minutes_since_first_lane": minutes_between(min(fired), now) if fired else None,
           "max_wait_minutes": lanes["max_wait_minutes"], "waiting": len(queued) + running,
           "start_documents": [job_message(run_id, key, detail, "documents") for key, detail in start],
           "start_browser": [], "jobs": jobs}
    if not picked:
        out["note"] = "no selected rows in the ledger for this run"
    emit(out)


# ---------------------------------------------------------------- mark-fired

def cmd_mark_fired(args) -> None:
    cfg = load_config()
    now = local_now(cfg)
    run_id = check_run_id(args.run_id, "nightly")
    key = check_key(args.key)
    ledger = load_ledger()
    require_job(ledger, run_id, key)
    if args.failed is not None:
        row = make_row(now, run_id, key, args.phase, "fire_failed", "", {"error": args.failed[:300]})
    else:
        row = make_row(now, run_id, key, args.phase, "fired", "", {})
    ledger.append([row])
    lane = ledger.lane(run_id, key, args.phase)
    failure = last_fire_failure(ledger.lane_rows(run_id, key, args.phase))
    emit({"key": key, "phase": args.phase,
          "fired_at": (last(lane["fired"]) or {}).get("at"), "fire_count": len(lane["fired"]),
          "fire_error": failure["detail"].get("error") if failure else None, "ledger_rows": [row]})


# ---------------------------------------------------------------- write-lane / check-claim

def files_problem(files) -> str | None:
    if files is None:
        return None
    if isinstance(files, dict):
        items = list(files.items())
    elif isinstance(files, list):
        items = [(str(i), v) for i, v in enumerate(files)]
    else:
        return "files must be an object of working-copy-relative paths"
    for name, value in items:
        if value is None:
            continue
        why = relative_path_problem(value)
        if why:
            return f"files.{name} {why} (e.g. cv/main_<name>.tex)"
    return None


def cmd_write_lane(args) -> None:
    cfg = load_config()
    now = local_now(cfg)
    run_id = check_run_id(args.run_id, "nightly")
    key = check_key(args.key)
    ledger = load_ledger()
    sel_row = require_job(ledger, run_id, key)
    try:
        data = json.load(sys.stdin)
    except ValueError as exc:
        fail(f"lane result is not valid JSON: {exc}")
    if not isinstance(data, dict):
        fail("lane result must be a JSON object")
    allowed = DOC_KEYS if args.phase == "documents" else BROWSER_KEYS
    statuses = DOC_STATUSES if args.phase == "documents" else BROWSER_STATUSES
    unknown = sorted(set(data) - allowed)
    if unknown:
        fail(f"unknown {args.phase} result fields: {unknown}; allowed: {sorted(allowed)}")
    status = data.get("status")
    if status not in statuses:
        fail(f"{args.phase} status must be one of {sorted(statuses)}")
    lane = ledger.lane(run_id, key, args.phase)
    if status == "running" and (lane["claimed"] or lane["finished"]):
        current = last(lane["finished"])["status"] if lane["finished"] else "running"
        fail(f"{args.phase} lane for {key} was already started ({current}); this start is a duplicate", code=3)
    if args.phase == "browser":
        doc = last(ledger.lane(run_id, key, "documents")["finished"])
        if not sel_row["detail"].get("browser"):
            fail("the browser phase is off for this job's run", code=4)
        if not doc or doc["status"] != "drafted":
            fail("this job has no drafted documents, so there is nothing to fill", code=4)
    row = data.get("tracker_row")
    if row is not None:
        if args.phase != "documents" or status != "drafted":
            fail("tracker_row belongs only on a documents result with status drafted")
        if not isinstance(row, dict) or set(row) - set(TRACKER_HEADER):
            fail(f"tracker_row keys must come from {TRACKER_HEADER}")
        if row.get("status") != "drafted":
            fail("tracker_row.status must be 'drafted' (submission is yours to record)")
        if not str(row.get("company") or "").strip() or not str(row.get("role") or "").strip():
            fail("tracker_row needs company and role")
        try:
            fit = int(str(row.get("fit_rating")))
        except ValueError:
            fail("tracker_row.fit_rating must be a bare number 0-100")
        if not 0 <= fit <= 100:
            fail("tracker_row.fit_rating must be 0-100")
    why = files_problem(data.get("files"))
    if why:
        fail(why)
    if isinstance(data.get("folder"), str) and data["folder"].strip():
        why = relative_path_problem(data["folder"])
        if why:
            fail(f"folder {why} (e.g. documents/applications/<name>)")

    if status == "running":
        nonce = secrets.token_hex(16)
        claim = make_row(now, run_id, key, args.phase, "claimed", "running", {"nonce": nonce})
        ledger.append([claim])
        emit({"claimed": True, "nonce": nonce, "phase": args.phase, "status": status, "ledger_rows": [claim]})
        return

    data.update({"key": key, "pipeline_run": run_id, "phase": args.phase, "updated_at": stamp(now)})
    size = len(canonical_json(data)) + 1
    if size > SHEET_CELL_LIMIT:
        fail(f"lane result is {size} characters, over the Sheet's {SHEET_CELL_LIMIT}-character cell limit: "
             "shorten notes, questions or please_check and write it again")
    path = result_path(run_id, key, args.phase)
    write_json(path, data)
    done = make_row(now, run_id, key, args.phase, "finished", status, data)
    ledger.append([done])
    emit({"written": rel(path), "phase": args.phase, "status": status, "ledger_rows": [done]})


def cmd_check_claim(args) -> None:
    run_id = check_run_id(args.run_id, "nightly")
    key = check_key(args.key)
    ledger = load_ledger()
    claims = ledger.lane(run_id, key, args.phase)["claimed"]
    owner = claims[0]["detail"].get("nonce") if claims else None
    ok = bool(args.nonce) and owner == args.nonce
    emit({"run_id": run_id, "key": key, "phase": args.phase, "ok": ok, "claims": len(claims)})
    sys.exit(0 if ok else 3)


# ---------------------------------------------------------------- record (append-tracker)

def open_tracker_match(trow: dict, rows: list[dict]) -> dict | None:
    """/apply Step 6b's append rule: an open row (status not final) with the same company and role."""
    return next((r for r in rows
                 if norm_text(r.get("role")) == norm_text(trow.get("role"))
                 and same_company(r.get("company"), trow.get("company"))
                 and str(r.get("status") or "").strip().lower() not in FINAL_STATUSES), None)


def cmd_record(args) -> None:
    """Append each drafted lane's tracker_row once. A lane that already has a "recorded" row is
    never appended again, even when the applicant later deleted that tracker row; a lane whose
    company and role already have an open tracker row is skipped (and checked again next time)."""
    if args.run_id:
        check_run_id(args.run_id, "nightly")
    cfg = load_config()
    now = local_now(cfg)
    ledger = load_ledger()
    header = list(TRACKER_HEADER) if args.dry_run else ensure_tracker_header()
    rows, _ = read_tracker()
    latest: dict[tuple[str, str], dict] = {}
    for row in ledger.rows:
        if row["phase"] != "documents" or row["event"] != "finished" or not RUN_RE.match(row["run_id"]):
            continue
        if args.run_id and row["run_id"] != args.run_id:
            continue
        latest[(row["run_id"], row["key"])] = row  # a later result for the same lane replaces it
    runs_checked: list[str] = []
    actions, added, new_rows = [], [], []
    already = recorded = 0
    for (rid, key), row in latest.items():
        trow = row["detail"].get("tracker_row")
        if row["status"] != "drafted" or not isinstance(trow, dict) or not trow:
            continue
        if rid not in runs_checked:
            runs_checked.append(rid)
        if ledger.lane(rid, key, "documents")["recorded"]:
            recorded += 1
            continue
        match = open_tracker_match(trow, rows)
        if match is not None:
            already += 1
            actions.append({"run": rid, "key": key, "action": "skipped",
                            "why": f"an open tracker row for {trow.get('company')} / {trow.get('role')} already "
                                   f"exists (status {match.get('status')}); left it unchanged"})
            continue
        if not args.dry_run:
            append_tracker_row(trow, header)
            new_rows.append(make_row(now, rid, key, "documents", "recorded", "",
                                     {"company": trow.get("company"), "role": trow.get("role")}))
        rows.append(trow)
        added.append({h: str(trow.get(h, "") or "") for h in header})
        actions.append({"run": rid, "key": key, "action": "appended" if not args.dry_run else "would append",
                        "company": trow.get("company"), "role": trow.get("role")})
    ledger.append(new_rows)
    emit({"runs_checked": runs_checked, "actions": actions, "added": added, "already_in_tracker": already,
          "already_recorded": recorded, "ledger_rows": new_rows})


# ---------------------------------------------------------------- fill runs

def cmd_fill_start(args) -> None:
    cfg = load_config()
    lanes = lane_settings(cfg)
    now = local_now(cfg)
    base = args.date or now.date().isoformat()
    if not valid_date(base):
        fail(f"--date must be YYYY-MM-DD, got {base!r}")
    ledger = load_ledger()
    active = active_runs(ledger, "fill", now, lanes["max_wait_minutes"])
    if active:
        fail(f"fill run {active[0]} started less than {lanes['max_wait_minutes']} minutes ago and has no "
             "run_finished row, so another fill run is not started", code=5, active_runs=active)
    fill_id = new_run_id(ledger, f"fill-{base}", FILLS)
    path = FILLS / fill_id
    path.mkdir(parents=True, exist_ok=True)
    nonce = secrets.token_hex(16)  # as in new-run: tells concurrent fill starts with one id apart
    row = make_row(now, fill_id, "", "run", "started", "",
                   {"test": False, "max_applications": None, "nonce": nonce})
    ledger.append([row])
    emit({"fill_id": fill_id, "dir": rel(path), "nonce": nonce, "ledger_rows": [row]})


def cmd_fill_finish(args) -> None:
    finish(check_run_id(args.fill_id, "fill"), args.aborted, "fill_id", args.nonce)


def draft_tracker_row(run_id: str, detail: dict, result: dict, rows: list[dict]) -> dict | None:
    """The tracker row that stands for a drafted job: the last row with the same posting URL,
    else the last with the same company and role dated on or after the run (an older row with
    that company and role is an earlier application, not this one)."""
    trow = result.get("tracker_row") if isinstance(result.get("tracker_row"), dict) else {}
    url = norm_url(detail.get("url")) or norm_url(trow.get("source"))
    company = trow.get("company") or detail.get("company")
    role = trow.get("role") or detail.get("title")
    run_day = parse_date(run_id)
    by_url, by_role = [], []
    for row in rows:
        if url and norm_url(row.get("source")) == url:
            by_url.append(row)
        elif norm_text(role) and norm_text(row.get("role")) == norm_text(role) and same_company(row.get("company"), company):
            day = parse_date(row.get("date"))
            if day is None or run_day is None or day >= run_day:
                by_role.append(row)
    return last(by_url) or last(by_role)


def cmd_fill_queue(args) -> None:
    """One entry per job key across every nightly run: a job drafted on two runs (a retried
    lane that finished late) is offered once, from its newest draft, and never while any
    run's browser lane for it has finished or is live."""
    cfg = load_config()
    sel = selection_settings(cfg)
    lanes = lane_settings(cfg)
    now = local_now(cfg)
    max_age = args.max_age_days if args.max_age_days is not None else lanes["max_fill_age_days"]
    if max_age < 0:
        fail("--max-age-days must be a whole number >= 0")
    # A browser lane fills one form; one that hasn't finished after browser_max_minutes has died.
    # Counting it as running for the whole max_wait_minutes would hold up every other job's filling.
    max_wait = lanes["browser_max_minutes"]
    ledger = load_ledger()
    tracker_rows, _ = read_tracker()
    position = {id(row): i for i, row in enumerate(ledger.rows)}

    def live(row: dict) -> bool:
        moment = parse_time(row["at"])
        return moment is not None and minutes_between(moment, now) < max_wait

    by_key: dict[str, list[tuple]] = {}
    for run_id in ledger.selection_runs():
        for key, sel_row in ledger.selected(run_id).items():
            detail = sel_row["detail"]
            if not detail.get("browser"):
                continue
            doc = last(ledger.lane(run_id, key, "documents")["finished"])
            if not doc or doc["status"] != "drafted":
                continue
            by_key.setdefault(key, []).append((run_id, detail, doc, ledger.lane(run_id, key, "browser")))

    ready, held = [], []
    running = 0
    for key, drafts in by_key.items():
        lanes_rows = [blane for *_, blane in drafts]
        if any(blane["finished"] for blane in lanes_rows):
            continue  # filled (or tried) from one of its runs already
        if any(live(r) for blane in lanes_rows for r in blane["claimed"] + blane["fired"]):
            running += 1
            continue
        # The newest draft carries the job message; its run id is the one the lane writes under.
        run_id, detail, doc, blane = max(drafts, key=lambda d: (parse_time(d[2]["at"]) or datetime.min.replace(
            tzinfo=timezone.utc), run_sort_key(d[0])))
        finished_at = parse_time(doc["at"])
        if finished_at is None or (now.date() - finished_at.astimezone(now.tzinfo).date()).days > max_age:
            continue
        msg = job_message(run_id, key, detail, "browser")
        claimed = next((d for d in drafts if d[3]["claimed"]), None)
        if claimed is not None:
            held.append({**job_message(claimed[0], key, claimed[1], "browser"),
                         "reason": "a browser lane claimed this job and never finished: check its Chrome tab"})
            continue
        why = files_problem(doc["detail"].get("files"))
        if why:
            held.append({**msg, "reason": f"the drafted result's {why}, so a browser lane can't pull them "
                                          "(a migrated draft?): fill it by hand"})
            continue
        trow = draft_tracker_row(run_id, detail, doc["detail"], tracker_rows)
        status = str((trow or {}).get("status") or "").strip().lower()
        if status in SUBMITTED_STATUSES or status == "withdrawn":
            held.append({**msg, "reason": f"the tracker shows this job as {status}: not filled again"})
            continue
        fires = sum(len(blane["fired"]) for blane in lanes_rows)
        if fires >= sel["max_lane_attempts"]:
            held.append({**msg, "reason": f"browser lane started {fires} times and never claimed the job"})
            continue
        fire_rows = sorted((r for blane in lanes_rows for r in blane["fired"] + blane["fire_failed"]),
                           key=lambda r: position.get(id(r), 0))
        failure = last_fire_failure(fire_rows)
        if failure is not None and live(failure):
            held.append({**msg, "reason": f"browser lane failed to start: {failure['detail'].get('error')}"})
            continue
        ready.append((finished_at, msg))
    ready.sort(key=lambda pair: pair[0], reverse=True)  # newest drafts first
    out = {"ready": [msg for _, msg in ready], "running": running, "start_browser": [], "held": held,
           "max_age_days": max_age, "tracker_checked": TRACKER.exists()}
    if not out["tracker_checked"]:
        out["warning"] = (f"{rel(TRACKER)} is missing, so the tracker was not checked: a job already "
                          f"applied for or withdrawn can't be held and may be offered again. Export the "
                          f"tracker tab into the working copy, then run fill-queue again")
    if not lane_stages(cfg, "browser"):
        out["ready"] = []
        out["note"] = "the browser phase is off in pipeline/config.yaml"
    out["start_browser"] = out["ready"][:max(0, lanes["max_parallel_browser"] - running)]
    emit(out)


# ---------------------------------------------------------------- stats

def cmd_stats(args) -> None:
    cfg = load_config()
    today = local_now(cfg).date()
    ledger = load_ledger()
    rows, _ = read_tracker()
    outcomes: dict[str, int] = {}
    mature = {"submitted": 0, "not_submitted": 0, "withdrawn": 0, "missing_from_tracker": 0}
    fresh = 0
    for run_id in ledger.selection_runs():
        run_date = parse_date(run_id)
        for key in ledger.selected(run_id):
            doc = ledger.lane(run_id, key, "documents")
            fin = last(doc["finished"])
            if fin:
                state = fin["status"]
            elif doc["claimed"]:
                state = "running"
            elif doc["fired"]:
                state = "unfinished"
            else:
                state = "not started"
            if state == "drafted":
                br = last(ledger.lane(run_id, key, "browser")["finished"])
                state = "filled" if br and br["status"] == "filled" else "drafted, not filled"
            outcomes[state] = outcomes.get(state, 0) + 1
            # Only drafts that record added to the tracker count, as before the ledger: a draft
            # skipped for an open row (or never recorded) has no tracker row of its own.
            rec = last(doc["recorded"])
            if rec is None:
                continue
            if run_date and (today - run_date).days < args.min_age_days:
                fresh += 1
                continue
            matches = [r for r in rows
                       if norm_text(r.get("role")) == norm_text(rec["detail"].get("role"))
                       and same_company(r.get("company"), rec["detail"].get("company"))]
            if not matches:
                mature["missing_from_tracker"] += 1
                continue
            status = str(matches[-1].get("status") or "").strip().lower()
            if status in SUBMITTED_STATUSES:
                mature["submitted"] += 1
            elif status == "withdrawn":
                mature["withdrawn"] += 1
            else:
                mature["not_submitted"] += 1
    decided = mature["submitted"] + mature["not_submitted"] + mature["withdrawn"]
    rate = round(100 * mature["submitted"] / decided) if decided else None
    line = (f"Pick quality: {mature['submitted']} of {decided} pipeline drafts older than "
            f"{args.min_age_days} days were submitted ({rate}%)." if decided else
            f"Pick quality: no pipeline drafts older than {args.min_age_days} days yet.")
    emit({"line": line, "submitted_rate_percent": rate, "drafts_older_than_min_age": mature,
          "drafts_too_recent_to_judge": fresh, "lane_outcomes_all_runs": outcomes})


# ---------------------------------------------------------------- ledger-from-runs

def read_json_or_flag(path: Path):
    """(data, None) / (None, None) when missing / (None, why) when unreadable."""
    if not path.exists():
        return None, None
    try:
        return read_json(path), None
    except (OSError, ValueError) as exc:
        return None, f"{path.name} is not valid JSON ({exc.__class__.__name__})"


def migrated_nonce(run_id: str, key: str, phase: str) -> str:
    return hashlib.sha256(f"ledger-from-runs:{run_id}:{key}:{phase}".encode("utf-8")).hexdigest()[:32]


def rows_from_run(folder: Path, run_id: str, tz, seq: list[int]) -> tuple[list[tuple], list[str]]:
    """Ledger rows rebuilt from one old run folder, as (moment, event rank, seq, row) tuples."""
    warnings: list[str] = []
    run_info, why = read_json_or_flag(folder / "run.json")
    if why:
        warnings.append(f"{run_id}: {why}")
    manifest, why = read_json_or_flag(folder / "manifest.json")
    if why:
        warnings.append(f"{run_id}: {why}; its picks can't be migrated")
    run_info = run_info if isinstance(run_info, dict) else {}
    manifest = manifest if isinstance(manifest, dict) else {}
    run_start = (parse_time(run_info.get("started_at")) or parse_time(manifest.get("created_at"))
                 or datetime.fromisoformat(run_id[:10]).replace(tzinfo=tz))
    created = max(parse_time(manifest.get("created_at")) or run_start, run_start)
    out: list[tuple] = []

    def add(moment, key, phase, event, status="", detail=None):
        moment = max(moment or created, created) if phase != "run" else moment
        seq[0] += 1
        out.append((moment, EVENT_RANK[event], seq[0],
                    {"row": None, "at": stamp(moment), "run_id": run_id, "key": key, "phase": phase,
                     "event": event, "status": status, "detail": detail if detail is not None else {}}))

    add(run_start, "", "run", "started", "", {"test": False, "max_applications": None})
    # The old pipeline ran the browser phase inside the run, and its results name absolute local
    # paths, so migrated picks never go to the morning filler.
    browser_on = False
    known: set[str] = set()
    for job in manifest.get("jobs") or []:
        key = job.get("key") if isinstance(job, dict) else None
        if not isinstance(key, str) or not KEY_RE.match(key):
            warnings.append(f"{run_id}: a job with an unusable key was skipped")
            continue
        known.add(key)
        add(created, key, "documents", "selected", "",
            {"url": job.get("url"), "title": job.get("title"), "company": job.get("company"),
             "rank_score": job.get("rank_score"), "rank_verdict": job.get("rank_verdict"),
             "attempt": job.get("attempt", 1), "browser": browser_on})
        for prefix, phase in (("", "documents"), ("browser_", "browser")):
            fired_at = parse_time(job.get(f"{prefix}fired_at"))
            if job.get(f"{prefix}fired_at") and fired_at is None:
                warnings.append(f"{run_id}/{key}: {prefix}fired_at is not a time; left out")
            if fired_at is not None:
                count = job.get(f"{prefix}fire_count")
                for _ in range(count if is_whole(count) and count > 1 else 1):
                    add(fired_at, key, phase, "fired")
            error = job.get(f"{prefix}fire_error")
            if error:
                # No time was kept for a failed start; it came after any successful one.
                add(max(fired_at, created) if fired_at else created, key, phase, "fire_failed", "",
                    {"error": str(error)[:300]})
            path = folder / "lanes" / f"{key}{'.json' if phase == 'documents' else '.browser.json'}"
            result, why = read_json_or_flag(path)
            if why:  # the old code read an unreadable result as an error result
                warnings.append(f"{run_id}/lanes/{why}; migrated as an error result")
                result = {"status": "error", "reason": "lane result file is not valid JSON"}
            if result is None:
                continue
            if not isinstance(result, dict):
                warnings.append(f"{run_id}/{path.name}: not a JSON object; left out")
                continue
            moment = parse_time(result.get("updated_at")) or fired_at or created
            status = result.get("status")
            add(moment, key, phase, "claimed", "running", {"nonce": migrated_nonce(run_id, key, phase)})
            finals = DOC_FINAL if phase == "documents" else BROWSER_FINAL
            if status in finals:
                if len(canonical_json(result)) + 1 > SHEET_CELL_LIMIT:
                    result = {k: result[k] for k in CORE_RESULT_KEYS if k in result}
                    if len(canonical_json(result)) + 1 > SHEET_CELL_LIMIT:
                        result = {"status": status, "reason": str(result.get("reason") or "")[:300]}
                    warnings.append(f"{run_id}/{path.name}: too long for one Sheet cell; migrated "
                                    "without its long fields (the full result stays in the run folder)")
                add(moment, key, phase, "finished", status, result)
                trow = result.get("tracker_row") if isinstance(result.get("tracker_row"), dict) else {}
                if phase == "documents" and job.get("recorded"):
                    add(moment, key, phase, "recorded", "",
                        {"company": job.get("tracker_company") or trow.get("company"),
                         "role": job.get("tracker_role") or trow.get("role")})
            elif status != "running":
                warnings.append(f"{run_id}/{path.name}: status {status!r} is not a final status; "
                                "migrated as a claim only")
    lanes_dir = folder / "lanes"
    if lanes_dir.is_dir():
        for path in sorted(lanes_dir.glob("*.json")):
            key = path.name[:-len(".browser.json")] if path.name.endswith(".browser.json") else path.stem
            if key not in known:
                warnings.append(f"{run_id}/lanes/{path.name}: no job with this key in the manifest; left out")
    end = max(moment for moment, *_ in out)
    if (folder / "report.md").exists():
        add(end, "", "run", "run_finished", "done", {"reason": "migrated from pipeline/runs"})
    else:
        add(end, "", "run", "run_finished", "aborted",
            {"reason": "migrated from pipeline/runs: the run never wrote report.md"})
    return out, warnings


def cmd_ledger_from_runs(args) -> None:
    cfg = load_config()
    tz = config_tz(cfg)
    runs_dir = Path(args.runs_dir).expanduser().resolve() if args.runs_dir else RUNS
    if not runs_dir.is_dir():
        fail(f"{args.runs_dir or rel(RUNS)} is not a folder")
    ledger = load_ledger(required=False)
    existing = ledger.run_ids()
    collected: list[tuple] = []
    migrated, skipped_runs, warnings = [], [], []
    seq = [0]
    folders = sorted((p for p in runs_dir.iterdir() if p.is_dir()), key=lambda p: run_sort_key(p.name))
    for folder in folders:
        run_id = folder.name
        if not RUN_RE.match(run_id):
            skipped_runs.append({"run": run_id, "reason": "not a run folder name"})
            continue
        if run_id in existing:
            skipped_runs.append({"run": run_id, "reason": "already in the ledger"})
            continue
        rows, run_warnings = rows_from_run(folder, run_id, tz, seq)
        collected.extend(rows)
        warnings.extend(run_warnings)
        migrated.append(run_id)
    collected.sort(key=lambda t: (t[0], t[1], t[2]))
    new_rows = [row for *_, row in collected]
    if not args.dry_run:
        ledger.append(new_rows)
    events: dict[str, int] = {}
    for row in new_rows:
        events[row["event"]] = events.get(row["event"], 0) + 1
    emit({"runs_dir": rel(runs_dir), "migrated_runs": migrated, "skipped_runs": skipped_runs,
          "rows_added": 0 if args.dry_run else len(new_rows), "rows_found": len(new_rows), "events": events,
          "warnings": warnings, "written": None if args.dry_run or not new_rows else rel(LEDGER),
          "dry_run": bool(args.dry_run)})


# ---------------------------------------------------------------- main

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", help="working-copy root (default: the folder above pipeline/)")
    parser.add_argument("--now", help="ISO-8601 time to treat as now (default: the real time in the "
                                      "configured time zone); a time without an offset is local")
    shared = argparse.ArgumentParser(add_help=False)  # the same options, accepted after the subcommand
    shared.add_argument("--root", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    shared.add_argument("--now", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add(name: str, help_text: str, func, aliases=()):
        p = sub.add_parser(name, help=help_text, parents=[shared], aliases=list(aliases))
        p.set_defaults(func=func)
        return p

    add("check", "validate pipeline/config.yaml", cmd_check)

    p = add("new-run", "start a nightly run: print its id and append started", cmd_new_run)
    p.add_argument("--date", help="YYYY-MM-DD (default: today in the configured time zone)")
    p.add_argument("--test", action="store_true", help="mark the run as a test run")
    p.add_argument("--max-applications", type=int, help="cap this run's picks (test runs)")

    p = add("check-run", "exit 0 if this session owns the earliest active run, else 5", cmd_check_run)
    p.add_argument("--run-id", required=True)
    p.add_argument("--nonce", help="the nonce new-run or fill-start printed (tells a concurrent start "
                                   "with the same id apart)")

    p = add("select", "pick tonight's jobs, write the manifest, append selected rows", cmd_select)
    p.add_argument("--run-id", default="")
    p.add_argument("--today", help="YYYY-MM-DD override for date checks")
    p.add_argument("--dry-run", action="store_true", help="print the selection without writing")
    p.add_argument("--force", action="store_true",
                   help="redo a selection whose picks never reached the ledger")
    p.add_argument("--no-browser", metavar="REASON", help="turn the browser phase off for these picks, with a short reason")

    p = add("lanes", "documents lane progress, and which lanes to start next", cmd_lanes)
    p.add_argument("--run-id", required=True)

    p = add("mark-fired", "record that a lane session was started", cmd_mark_fired)
    p.add_argument("--run-id", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--phase", choices=PHASES, default="documents")
    p.add_argument("--failed", metavar="REASON", help="record a failed start instead, with this short reason")

    p = add("write-lane", "claim a lane ({\"status\": \"running\"}) or store its result (JSON on stdin)",
            cmd_write_lane)
    p.add_argument("--run-id", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--phase", choices=PHASES, default="documents")

    p = add("check-claim", "exit 0 if the earliest claim on this lane carries this nonce, else 3", cmd_check_claim)
    p.add_argument("--run-id", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--phase", choices=PHASES, default="documents")
    p.add_argument("--nonce", required=True)

    p = add("record", "append drafted tracker rows from finished documents lanes of every run", cmd_record,
            aliases=("append-tracker",))
    p.add_argument("--run-id", help="only this run's lanes (default: every run, so late drafts are included)")
    p.add_argument("--all", action="store_true", help="every run (the default; kept for the old spelling)")
    p.add_argument("--dry-run", action="store_true")

    p = add("finish-run", "append run_finished for a nightly run", cmd_finish_run)
    p.add_argument("--run-id", required=True)
    p.add_argument("--aborted", metavar="REASON", help="the run stopped early, for this reason")
    p.add_argument("--nonce", help="new-run's nonce: append nothing when another session owns the run id")

    p = add("fill-start", "start a morning fill run: print its id and append started", cmd_fill_start)
    p.add_argument("--date", help="YYYY-MM-DD (default: today in the configured time zone)")

    p = add("fill-queue", "drafted jobs ready for a browser lane, and which to start now", cmd_fill_queue)
    p.add_argument("--max-age-days", type=int, help="only drafts finished within this many days "
                                                    "(default: lanes.max_fill_age_days)")

    p = add("fill-finish", "append run_finished for a fill run", cmd_fill_finish)
    p.add_argument("--fill-id", required=True)
    p.add_argument("--aborted", metavar="REASON", help="the fill run stopped early, for this reason")
    p.add_argument("--nonce", help="fill-start's nonce: append nothing when another session owns the fill id")

    p = add("stats", "pick quality across runs", cmd_stats)
    p.add_argument("--min-age-days", type=int, default=7)

    p = add("ledger-from-runs", "one-time migration: ledger rows from old pipeline/runs folders",
            cmd_ledger_from_runs)
    p.add_argument("--runs-dir", help="the old runs folder (default: pipeline/runs)")
    p.add_argument("--dry-run", action="store_true", help="count the rows without writing them")
    return parser


def main(argv: list[str] | None = None) -> None:
    global NOW_OVERRIDE
    args = build_parser().parse_args(argv)
    if args.root:
        root = Path(args.root).expanduser()
        if not root.is_dir():
            fail(f"--root {args.root!r} is not a folder")
        set_root(root)
    if args.now:
        moment = parse_iso(args.now)
        if moment is None:
            fail(f"--now must be an ISO-8601 time such as 2026-11-09T00:30:00-08:00, got {args.now!r}")
        NOW_OVERRIDE = moment
    if args.cmd == "select" and not args.dry_run and not args.run_id:
        fail("select needs --run-id (or --dry-run)")
    args.func(args)


if __name__ == "__main__":
    main()
