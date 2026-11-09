"""Tests for pipeline/drive_sync.py - the file layer between a working copy and Drive/Sheets.

Sessions reach Drive and Sheets only through connector tools the model calls; drive_sync.py
does the deterministic part: what each role pulls and may push, local files to Sheet writes
and Sheet reads back to local files, checksums of downloads. These drive it through
subprocess in temp working copies (`--root`) and simulate the Sheet as measured: writes are
USER_ENTERED, so a leading apostrophe forces plain text and is not returned on read; reads
omit trailing empty cells and arrive in pages of the documented get_values shape
(`{"range": "seen_jobs!A1:M200", "values": [...]}`).
"""
import base64
import csv
import hashlib
import importlib.util
import io
import json
import re
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

REPO = Path(__file__).resolve().parent.parent
TOOL = REPO / "pipeline" / "drive_sync.py"

_spec = importlib.util.spec_from_file_location("drive_sync_under_test", TOOL)
ds = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ds)

RUN = "2026-11-09"
TRICKY = ["007", "=1+1", "+1 555 0100", "'x", "''x", "TRUE", "3.50", "", "line one\nline two",
          "tab\tseparated", '{"b": 2, "a": [1, "x"]}', "Zürich, 東京"]


def b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def seen_entries() -> dict:
    return {
        "linkedin-1001": {
            "title": "ML Engineer", "company": "Acme Corp", "url": "https://example.com/jobs/1001",
            "first_seen": "2026-11-01", "status": "ranked", "rank_score": 82, "rank_verdict": "apply",
            "location": "Springfield", "posted_date": "2026-10-30", "rank_date": "2026-11-02",
            "deadline": None, "portal": "linkedin-search", "strengths": ["evals", "agents"], "gaps": [],
        },
        "linkedin-1002": {
            "title": "=1+1", "company": "007", "location": "+1 555 0100", "status": "new",
            "rank_verdict": "TRUE", "salary": "3.50", "notes": "'x", "url": "https://example.com/jobs/1002",
            "first_seen": "2026-11-01", "deadline": "2026-12-01", "remote": False,
        },
        "board-77": {
            "title": "Data Scientist\nApplied AI", "company": "Globex\tLabs", "location": "Zürich",
            "status": "skipped", "rank_score": 3.5, "url": "https://example.com/jobs/77",
            "first_seen": "2026-11-03", "summary": "東京 office; \"quoted\"; ends with a space ",
            "deadline": None,
        },
        "board-78": {"title": "", "company": "Initech", "url": "https://example.com/jobs/78",
                     "first_seen": "2026-11-04", "status": "new", "deadline": None},
        "board-79": {"title": "Analyst", "company": "Umbrella", "url": "https://example.com/jobs/79",
                     "first_seen": "2026-11-04", "status": "new", "deadline": None, "extra": {"z": 1, "a": [True, None]}},
    }


def tracker_rows() -> list:
    return [
        ["2026-11-01", "Acme Corp", "Software", "ML Engineer", "full-time", "linkedin", "drafted", "",
         "8/10", 'Said "hi", then\nleft a second line', "cv/main_acme_ml_engineer.tex",
         "cover_letters/cover_acme_ml_engineer.tex", "nightly", ""],
        ["2026-11-02", "Globex", "", "Data Scientist", "", "", "drafted", "", "", "=1+1", "", "", "", ""],
        ["2026-11-03", "Initech", "Finance", "Analyst", "", "", "applied", "", "007", "+1 555 0100",
         "", "", "", "2026-12-01"],
    ]


def ledger_rows() -> list:
    def row(at, run_id, key, phase, event, status="", detail=None):
        return {"row": None, "at": at, "run_id": run_id, "key": key, "phase": phase, "event": event,
                "status": status, "detail": {} if detail is None else detail}

    job = "acme-ml-engineer"
    return [
        row("2026-11-09T08:00:00Z", RUN, "", "run", "started", detail={"test": False, "max_applications": None}),
        row("2026-11-09T08:05:00Z", RUN, job, "documents", "selected",
            detail={"url": "https://example.com/jobs/1001", "title": "ML Engineer", "company": "Acme",
                    "rank_score": 82, "rank_verdict": "apply", "attempt": 1, "browser": True}),
        row("2026-11-09T08:06:00Z", RUN, job, "documents", "fired"),
        row("2026-11-09T08:07:00+00:00", RUN, job, "documents", "claimed", "running", {"nonce": "a1b2c3"}),
        row("2026-11-09T09:00:00Z", RUN, job, "documents", "finished", "drafted",
            {"status": "drafted", "files": {"cv": "cv/main_acme_ml_engineer.tex"},
             "notes": "line one\nline two \"quoted\" =1+1 'x"}),
        row("2026-11-09T10:00:00Z", RUN, "", "run", "run_finished", "done", {"reason": None}),
        row("2026-11-10T07:30:00Z", "fill-2026-11-10", "", "run", "started", detail={"test": False}),
        row("2026-11-10T07:31:00Z", RUN, job, "browser", "claimed", "running", {"nonce": "d4e5"}),
    ]


def csv_text(rows: list, header=None) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header or ds.TRACKER_HEADER)
    writer.writerows(rows)
    return buf.getvalue()


class FakeSheet:
    """One tab as Sheets holds it after USER_ENTERED writes of apostrophe-prefixed text."""

    def __init__(self, test: unittest.TestCase, tab: str):
        self.test, self.tab = test, tab
        self.width = len(ds.HEADERS[tab])
        self.rows: list = []

    def stored(self, cell):
        self.test.assertIsInstance(cell, str)
        self.test.assertTrue(cell.startswith("'"), f"cell written without the apostrophe: {cell!r}")
        return cell[1:]  # the apostrophe forces plain text and is not returned on read

    def apply(self, plan: dict) -> None:
        for chunk in plan["append"]:
            self.test.assertEqual(chunk["range"], f"{self.tab}!A1")
            for row in chunk["values"]:
                self.test.assertEqual(len(row), self.width)
                self.rows.append([self.stored(c) for c in row])
        for update in plan["update"]:
            m = re.fullmatch(rf"{self.tab}!A(\d+):([A-Z]+)(\d+)", update["range"])
            self.test.assertIsNotNone(m, update["range"])
            self.test.assertEqual(m.group(1), m.group(3))
            self.test.assertEqual(m.group(2), ds.col_letter(self.width))
            self.test.assertEqual(len(update["values"]), 1)
            self.rows[int(m.group(1)) - 1] = [self.stored(c) for c in update["values"][0]]

    def pages(self, directory: Path, size: int = 200, quote: bool = False, tag: str = "page") -> list:
        """Saved get_values results, `size` rows a page, until a page comes back short."""
        last = ds.col_letter(self.width)
        sheet = f"'{self.tab}'" if quote else self.tab
        files, start = [], 1
        while True:
            end = start + size - 1
            values = []
            for row in self.rows[start - 1:end]:
                row = list(row)
                while row and row[-1] == "":
                    row.pop()  # trailing empty cells are omitted
                values.append(row)
            while values and not values[-1]:
                values.pop()  # and so are trailing empty rows
            page = {"range": f"{sheet}!A{start}:{last}{end}", "majorDimension": "ROWS"}
            if values:
                page["values"] = values
            path = directory / f"{tag}_{self.tab}_{start}.json"
            path.write_text(json.dumps(page, ensure_ascii=False), encoding="utf-8")
            files.append(path)
            if len(values) < size:
                return files
            start = end + 1


class DriveSyncCase(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        base = Path(self._tmp.name)
        self.root = base / "work"
        self.scratch = base / "scratch"
        self.root.mkdir()
        self.scratch.mkdir()
        self.write_config()

    def write_config(self, **drive_over):
        drive = {
            "root_id": "FOLDER_ID_ROOT", "sheet_id": "SHEET_ID_TEST", "folders": {"cv": "FOLDER_ID_CV"},
            "overlay": ["CLAUDE.md", "cv/main_example.tex",
                        ".claude/skills/job-application-assistant/01-candidate-profile.md"],
            "replaceable": ["job_scraper/notion_sync.json", "company_research/*.json"],
        }
        drive.update(drive_over)
        self.write("pipeline/config.yaml", json.dumps({"drive": drive}, indent=2))  # JSON is YAML

    def write(self, rel: str, text: str) -> Path:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def read(self, rel: str) -> str:
        return (self.root / rel).read_text(encoding="utf-8")

    def run_tool(self, *args, expect=0, root_first=True):
        root = ["--root", str(self.root)]
        argv = [*root, *map(str, args)] if root_first else [*map(str, args), *root]
        proc = subprocess.run([sys.executable, str(TOOL), *argv], capture_output=True, text=True,
                              encoding="utf-8")
        self.assertEqual(proc.returncode, expect, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
        self.stderr = proc.stderr
        return json.loads(proc.stdout) if proc.stdout.strip() else None

    def scratch_file(self, name: str, content: str) -> Path:
        path = self.scratch / name
        path.write_text(content, encoding="utf-8")
        return path

    def tool_output(self, name: str, text: str, file_id: str, wrap: bool = False) -> Path:
        result = {"content": b64(text), "id": file_id, "mimeType": "text/plain", "title": Path(name).name}
        payload = [{"type": "text", "text": json.dumps(result)}] if wrap else result
        return self.scratch_file(name + ".result.json", json.dumps(payload))

    def pull(self, rel: str, text: str, file_id: str) -> dict:
        """What the drive-sync pull procedure does: download, then decode with the checksum."""
        saved = self.tool_output(rel.replace("/", "_"), text, file_id)
        return self.run_tool("decode", "--tool-output", saved, "--out", rel, "--id", file_id,
                             "--description", f"sha256={sha(text)} size={len(text.encode('utf-8'))}")

    def load_tab(self, tab: str, local_text: str, size: int = 200, quote: bool = False) -> FakeSheet:
        """A first load into an empty tab, then the export that records the snapshot."""
        rel = ds.DEFAULT_FILES[tab]
        self.write(rel, local_text)
        plan = self.run_tool("rows", tab, "--from", rel)
        self.assertTrue(plan["header"])
        sheet = FakeSheet(self, tab)
        sheet.apply(plan)
        self.export(tab, sheet, size=size, quote=quote)
        return sheet

    def export(self, tab: str, sheet: FakeSheet, *extra, size=200, quote=False, expect=0, tag="page"):
        args = ["export", tab]
        for page in sheet.pages(self.scratch, size=size, quote=quote, tag=tag):
            args += ["--values", page]
        return self.run_tool(*args, *extra, expect=expect)


class Encoding(DriveSyncCase):
    def test_every_value_gets_exactly_one_leading_apostrophe(self):
        for value in TRICKY:
            self.assertEqual(ds.encode_cell(value), "'" + value)
        self.assertEqual(ds.encode_cell(None), "'")
        self.assertEqual(ds.encode_cell(True), "'true")
        self.assertEqual(ds.encode_cell(False), "'false")
        self.assertEqual(ds.encode_cell(82), "'82")
        self.assertEqual(ds.encode_cell(3.5), "'3.5")
        self.assertEqual(ds.encode_cell({"b": [1, "é"], "a": None}), '\'{"a":null,"b":[1,"é"]}')

    def test_canonical_json_is_sorted_compact_and_unescaped(self):
        self.assertEqual(ds.canonical({"b": 1, "a": {"d": "東京", "c": [True, None]}}),
                         '{"a":{"c":[true,null],"d":"東京"},"b":1}')

    def test_tricky_values_survive_a_sheet_round_trip(self):
        entries = {f"tricky-{i}": {"title": v, "company": v, "location": v, "notes": v, "url": "https://example.com"}
                   for i, v in enumerate(TRICKY)}
        doc = {"seen": entries}
        self.load_tab("seen_jobs", json.dumps(doc, ensure_ascii=False))
        exported = json.loads(self.read("job_scraper/seen_jobs.json"))
        self.assertEqual(json.dumps(exported, sort_keys=True), json.dumps(doc, sort_keys=True))

    def test_entry_json_cell_holds_canonical_json(self):
        entries = seen_entries()
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        plan = self.run_tool("rows", "seen_jobs")
        rows = [row for chunk in plan["append"] for row in chunk["values"]]
        self.assertEqual(rows[0], ["'" + h for h in ds.SEEN_HEADER])
        by_key = {row[0][1:]: row for row in rows[1:]}
        for key, entry in entries.items():
            self.assertEqual(by_key[key][-1], "'" + ds.canonical(entry))
        first = by_key["linkedin-1001"]
        self.assertEqual(first[1:4], ["'ranked", "'82", "'apply"])
        self.assertEqual(first[11], "'")  # deadline None


class ExportShape(DriveSyncCase):
    def page(self, name: str, rng: str, values) -> Path:
        page = {"range": rng, "majorDimension": "ROWS"}
        if values is not None:
            page["values"] = values
        return self.scratch_file(name, json.dumps(page, ensure_ascii=False))

    def test_rows_are_padded_to_the_header_width(self):
        page = self.page("t.json", "tracker!A1:N200", [ds.TRACKER_HEADER, ["2026-11-02", "Globex", "", "Data Scientist"],
                                                        ["2026-11-03", "Initech"]])
        out = self.run_tool("export", "tracker", "--values", page)
        self.assertEqual(out["rows"], 2)
        rows = list(csv.reader(io.StringIO(self.read("job_search_tracker.csv"))))
        self.assertEqual(rows[0], ds.TRACKER_HEADER)
        self.assertEqual(rows[1], ["2026-11-02", "Globex", "", "Data Scientist"] + [""] * 10)
        self.assertEqual(rows[2], ["2026-11-03", "Initech"] + [""] * 12)

    def test_header_mismatch_is_a_data_error(self):
        header = list(ds.TRACKER_HEADER)
        header[2] = "industry"
        page = self.page("t.json", "tracker!A1:N200", [header, ["2026-11-02", "Globex"]])
        self.run_tool("export", "tracker", "--values", page, expect=2)
        self.assertIn("header mismatch", self.stderr)
        self.assertFalse((self.root / "job_search_tracker.csv").exists())

    def test_a_missing_header_column_is_a_header_mismatch(self):
        page = self.page("t.json", "tracker!A1:N200", [ds.TRACKER_HEADER[:-1]])
        self.run_tool("export", "tracker", "--values", page, expect=2)
        self.assertIn("header mismatch", self.stderr)

    def test_quoted_sheet_name_and_pages_that_start_past_row_one(self):
        rows = [ds.TRACKER_HEADER] + [[f"2026-11-0{i}", f"Company {i}"] for i in range(1, 5)]
        first = self.page("p1.json", "'tracker'!A1:N3", rows[:3])
        second = self.page("p2.json", "tracker!A4:N6", rows[3:])
        out = self.run_tool("export", "tracker", "--values", first, "--values", second)
        self.assertEqual(out["rows"], 4)
        snapshot = json.loads(self.read(".drive_sync/snapshot_tracker.json"))
        self.assertEqual([r["row"] for r in snapshot["rows"]], [2, 3, 4, 5])

    def test_a_short_page_still_starts_at_its_range_row(self):
        rows = [ds.TRACKER_HEADER] + [["2026-11-01", "Acme"], ["2026-11-02", "Globex"]]
        first = self.page("p1.json", "tracker!A1:N2", rows[:2])
        second = self.page("p2.json", "tracker!A3:N4", rows[2:])  # one row of two
        self.run_tool("export", "tracker", "--values", first, "--values", second)
        snapshot = json.loads(self.read(".drive_sync/snapshot_tracker.json"))
        self.assertEqual([r["row"] for r in snapshot["rows"]], [2, 3])

    def test_the_first_page_must_start_at_row_one(self):
        page = self.page("p.json", "tracker!A201:N400", [["2026-11-01", "Acme"]])
        self.run_tool("export", "tracker", "--values", page, expect=2)
        self.assertIn("row 1", self.stderr)

    def test_pages_must_be_contiguous(self):
        first = self.page("p1.json", "tracker!A1:N2", [ds.TRACKER_HEADER, ["2026-11-01", "Acme"]])
        third = self.page("p3.json", "tracker!A5:N6", [["2026-11-05", "Globex"]])
        self.run_tool("export", "tracker", "--values", first, "--values", third, expect=2)
        self.assertIn("expected row 3", self.stderr)

    def test_a_full_last_page_asks_for_the_next_page(self):
        page = self.page("p.json", "tracker!A1:N2", [ds.TRACKER_HEADER, ["2026-11-01", "Acme"]])
        self.run_tool("export", "tracker", "--values", page, expect=2)
        self.assertIn("tracker!A3:N4", self.stderr)
        self.assertIn("--no-more-rows", self.stderr)
        self.assertFalse((self.root / "job_search_tracker.csv").exists())

    def test_a_full_last_page_at_the_end_of_the_sheet(self):
        # Appends grow the grid to fit, so the page after a full one can lie past the sheet's
        # end and its request fails; the caller says so and the full page is complete.
        page = self.page("p.json", "tracker!A1:N2", [ds.TRACKER_HEADER, ["2026-11-01", "Acme"]])
        self.assertEqual(self.run_tool("export", "tracker", "--values", page, "--no-more-rows")["rows"], 1)

    def test_a_result_nested_under_any_key_is_read(self):
        page = {"range": "tracker!A1:N200", "values": [ds.TRACKER_HEADER, ["2026-11-01", "Acme"]]}
        nested = self.scratch_file("n.json", json.dumps({"result": {"valueRange": page}}))
        self.assertEqual(self.run_tool("export", "tracker", "--values", nested)["rows"], 1)

    def test_a_page_from_another_tab_is_refused(self):
        page = self.page("p.json", "ledger!A1:N200", [ds.TRACKER_HEADER])
        self.run_tool("export", "tracker", "--values", page, expect=2)
        self.assertIn("not 'tracker'", self.stderr)

    def test_an_empty_tab_is_refused(self):
        page = self.page("p.json", "tracker!A1:N200", None)
        self.run_tool("export", "tracker", "--values", page, expect=2)
        self.assertIn("no header row", self.stderr)

    def test_a_saved_result_wrapped_in_content_blocks_is_read(self):
        page = {"range": "tracker!A1:N200", "values": [ds.TRACKER_HEADER, ["2026-11-01", "Acme"]]}
        wrapped = self.scratch_file("w.json", json.dumps([{"type": "text", "text": json.dumps(page)}]))
        self.assertEqual(self.run_tool("export", "tracker", "--values", wrapped)["rows"], 1)

    def test_data_beyond_the_last_column_is_refused(self):
        page = self.page("p.json", "tracker!A1:P200", [ds.TRACKER_HEADER, ["2026-11-01"] + [""] * 14 + ["stray"]])
        self.run_tool("export", "tracker", "--values", page, expect=2)
        self.assertIn("beyond column N", self.stderr)


class CellLimit(DriveSyncCase):
    def test_a_cell_of_exactly_50000_characters_is_written(self):
        row = ["2026-11-01", "Acme"] + [""] * 7 + ["x" * 49_999] + [""] * 4
        self.write("job_search_tracker.csv", csv_text([row]))
        plan = self.run_tool("rows", "tracker")
        cells = plan["append"][-1]["values"][-1]
        self.assertEqual(len(cells[9]), 50_000)

    def test_a_longer_cell_fails_naming_the_row_and_column(self):
        row = ["2026-11-01", "Acme"] + [""] * 7 + ["x" * 50_000] + [""] * 4
        self.write("job_search_tracker.csv", csv_text([row]))
        self.assertIsNone(self.run_tool("rows", "tracker", expect=2))
        self.assertIn("row 1", self.stderr)
        self.assertIn("column notes", self.stderr)

    def test_a_long_seen_jobs_entry_fails_naming_the_key(self):
        entries = seen_entries()
        entries["board-78"]["summary"] = "y" * 50_000
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        self.run_tool("rows", "seen_jobs", expect=2)
        self.assertIn("key board-78", self.stderr)
        self.assertIn("entry_json", self.stderr)


class RoundTrips(DriveSyncCase):
    def test_seen_jobs_round_trip_over_several_pages(self):
        doc = {"seen": seen_entries()}
        self.load_tab("seen_jobs", json.dumps(doc, indent=2, ensure_ascii=False) + "\n", size=2, quote=True)
        text = self.read("job_scraper/seen_jobs.json")
        exported = json.loads(text)
        self.assertEqual(json.dumps(exported, sort_keys=True), json.dumps(doc, sort_keys=True))
        self.assertEqual(list(exported["seen"]), list(doc["seen"]))  # Sheet order = local order
        self.assertEqual(text, json.dumps(exported, indent=2, ensure_ascii=False) + "\n")  # save_state format

    def test_tracker_round_trip_is_byte_exact(self):
        original = csv_text(tracker_rows())
        self.load_tab("tracker", original, size=2)
        self.assertEqual((self.root / "job_search_tracker.csv").read_bytes(), original.encode("utf-8"))

    def test_ledger_round_trip_assigns_row_numbers(self):
        rows = ledger_rows()
        self.load_tab("ledger", json.dumps({"rows": rows}), size=3)
        exported = json.loads(self.read("pipeline/ledger.json"))["rows"]
        self.assertEqual([r["row"] for r in exported], list(range(2, 2 + len(rows))))
        strip = [{k: v for k, v in r.items() if k != "row"} for r in exported]
        self.assertEqual(strip, [{k: v for k, v in r.items() if k != "row"} for r in rows])

    def test_header_is_appended_only_on_the_first_load(self):
        self.load_tab("ledger", json.dumps({"rows": ledger_rows()[:2]}))
        plan = self.run_tool("rows", "ledger")
        self.assertFalse(plan["header"])
        self.assertEqual(plan["append"], [])
        self.assertEqual(plan["counts"], {"append": 0, "update": 0, "unchanged": 2, "missing_locally": 0})

    def test_appends_are_chunked_by_rows_and_characters(self):
        entries = {f"job-{i:03d}": {"title": "t" * 3_000, "url": "https://example.com"} for i in range(30)}
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        plan = self.run_tool("rows", "seen_jobs")
        sizes = [len(chunk["values"]) for chunk in plan["append"]]
        self.assertEqual(sum(sizes), 31)  # header + 30
        for chunk in plan["append"]:
            self.assertLessEqual(len(json.dumps(chunk["values"], ensure_ascii=False)), 40_000 + 100)
        small = self.run_tool("rows", "seen_jobs", "--chunk", "4")
        self.assertTrue(all(len(c["values"]) <= 4 for c in small["append"]))


class Deltas(DriveSyncCase):
    def test_seen_jobs_append_update_unchanged_and_missing_locally(self):
        entries = seen_entries()
        sheet = self.load_tab("seen_jobs", json.dumps({"seen": entries}))
        entries["linkedin-1002"]["status"] = "ranked"
        entries["linkedin-1002"]["rank_score"] = 71
        removed = entries.pop("board-78")
        entries["board-80"] = {"title": "New", "company": "Hooli", "url": "https://example.com/80",
                               "first_seen": "2026-11-09", "status": "new", "deadline": None}
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        plan = self.run_tool("rows", "seen_jobs")
        self.assertFalse(plan["header"])
        self.assertEqual(plan["counts"], {"append": 1, "update": 1, "unchanged": 3, "missing_locally": 1})
        self.assertEqual(plan["update"][0]["key"], "linkedin-1002")
        self.assertEqual(plan["update"][0]["range"], "seen_jobs!A3:M3")  # header, then 1001, then 1002
        self.assertEqual(plan["append"][0]["values"][0][0], "'board-80")
        sheet.apply(plan)
        # The Sheet keeps board-78 (never removed), so the verify names it and writes nothing.
        before = self.read("job_scraper/seen_jobs.json")
        out = self.export("seen_jobs", sheet, "--verify-against", "job_scraper/seen_jobs.json", expect=2, tag="v1")
        self.assertFalse(out["match"])
        self.assertEqual(out["differences"], [{"key": "board-78", "row": 5, "issue": "only in the Sheet"}])
        self.assertEqual(self.read("job_scraper/seen_jobs.json"), before)
        entries["board-78"] = removed
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        before = self.read("job_scraper/seen_jobs.json")
        out = self.export("seen_jobs", sheet, "--verify-against", "job_scraper/seen_jobs.json", tag="v2")
        self.assertTrue(out["match"])
        self.assertEqual(self.read("job_scraper/seen_jobs.json"), before)  # snapshot only
        snapshot = json.loads(self.read(".drive_sync/snapshot_seen_jobs.json"))
        self.assertEqual(len(snapshot["rows"]), 6)
        again = self.run_tool("rows", "seen_jobs")
        self.assertEqual(again["counts"], {"append": 0, "update": 0, "unchanged": 6, "missing_locally": 0})

    def test_verify_against_reports_a_changed_entry(self):
        entries = seen_entries()
        sheet = self.load_tab("seen_jobs", json.dumps({"seen": entries}))
        sheet.rows[1][-1] = sheet.rows[1][-1].replace("ML Engineer", "ML Engineer II")
        out = self.export("seen_jobs", sheet, "--verify-against", "job_scraper/seen_jobs.json", expect=2)
        self.assertEqual(out["differences"], [{"key": "linkedin-1001", "row": 2, "issue": "differs"}])

    def test_browse_columns_are_ignored_on_export(self):
        entries = seen_entries()
        sheet = self.load_tab("seen_jobs", json.dumps({"seen": entries}))
        sheet.rows[1][1:12] = ["edited by hand"] * 11
        self.export("seen_jobs", sheet, "--verify-against", "job_scraper/seen_jobs.json")

    def test_tracker_positional_update_and_append(self):
        rows = tracker_rows()
        sheet = self.load_tab("tracker", csv_text(rows))
        rows[1][6] = "applied"
        rows.append(["2026-11-09", "Hooli", "", "Engineer", "", "", "drafted", "", "", "", "", "", "", ""])
        self.write("job_search_tracker.csv", csv_text(rows))
        plan = self.run_tool("rows", "tracker")
        self.assertEqual(plan["counts"], {"append": 1, "update": 1, "unchanged": 2, "missing_locally": 0})
        self.assertEqual(plan["update"][0]["range"], "tracker!A3:N3")
        sheet.apply(plan)
        self.export("tracker", sheet, "--verify-against", "job_search_tracker.csv", tag="v")

    def test_tracker_rows_removed_locally_are_refused(self):
        rows = tracker_rows()
        self.load_tab("tracker", csv_text(rows))
        self.write("job_search_tracker.csv", csv_text(rows[:2]))
        self.assertIsNone(self.run_tool("rows", "tracker", expect=2))
        self.assertIn("rows were removed locally", self.stderr)
        self.assertIn("never cleared", self.stderr)

    def test_tracker_verify_against_names_the_row(self):
        rows = tracker_rows()
        sheet = self.load_tab("tracker", csv_text(rows))
        sheet.rows[2][9] = "edited in the Sheet"
        out = self.export("tracker", sheet, "--verify-against", "job_search_tracker.csv", expect=2)
        self.assertEqual(out["differences"], [{"position": 2, "row": 3, "issue": "differs"}])

    def test_ledger_appends_only_new_rows(self):
        rows = ledger_rows()
        sheet = self.load_tab("ledger", json.dumps({"rows": rows[:5]}))
        local = json.loads(self.read("pipeline/ledger.json"))
        local["rows"] += rows[5:]
        self.write("pipeline/ledger.json", json.dumps(local))
        plan = self.run_tool("rows", "ledger")
        self.assertEqual(plan["counts"], {"append": 3, "update": 0, "unchanged": 5, "missing_locally": 0})
        self.assertEqual(plan["update"], [])
        sheet.apply(plan)
        self.export("ledger", sheet, tag="again")
        self.assertEqual([r["row"] for r in json.loads(self.read("pipeline/ledger.json"))["rows"]], list(range(2, 10)))

    def test_ledger_verify_compares_content_not_row_numbers(self):
        rows = ledger_rows()
        sheet = self.load_tab("ledger", json.dumps({"rows": rows}))
        self.write("pipeline/pre_push.json", json.dumps({"rows": rows}))  # still "row": null
        self.assertTrue(self.export("ledger", sheet, "--verify-against", "pipeline/pre_push.json", tag="v")["match"])
        sheet.rows.append(ds.ledger_cells(dict(rows[2], at="2026-11-09T11:00:00Z")))
        out = self.export("ledger", sheet, "--verify-against", "pipeline/pre_push.json", expect=2, tag="w")
        self.assertEqual(out["differences"], [{"position": 9, "row": 10, "issue": "only in the Sheet"}])

    def test_ledger_row_that_differs_from_the_snapshot_is_refused(self):
        self.load_tab("ledger", json.dumps({"rows": ledger_rows()[:3]}))
        local = json.loads(self.read("pipeline/ledger.json"))
        local["rows"][2]["event"] = "fire_failed"
        self.write("pipeline/ledger.json", json.dumps(local))
        self.run_tool("rows", "ledger", expect=2)
        self.assertIn("row 4 differs", self.stderr)

    def test_ledger_row_number_unknown_to_the_snapshot_is_refused(self):
        self.load_tab("ledger", json.dumps({"rows": ledger_rows()[:3]}))
        local = json.loads(self.read("pipeline/ledger.json"))
        local["rows"][2]["row"] = 40
        self.write("pipeline/ledger.json", json.dumps(local))
        self.run_tool("rows", "ledger", expect=2)
        self.assertIn("row 40 was not in the Sheet", self.stderr)

    def test_a_ledger_row_number_used_twice_is_refused(self):
        self.load_tab("ledger", json.dumps({"rows": ledger_rows()[:3]}))
        local = json.loads(self.read("pipeline/ledger.json"))
        local["rows"].append(dict(local["rows"][2]))
        self.write("pipeline/ledger.json", json.dumps(local))
        self.run_tool("rows", "ledger", expect=2)
        self.assertIn("row 4 appears twice", self.stderr)

    def test_numbered_ledger_rows_without_a_snapshot_are_refused(self):
        rows = ledger_rows()[:2]
        rows[0]["row"] = 2
        self.write("pipeline/ledger.json", json.dumps({"rows": rows}))
        self.run_tool("rows", "ledger", expect=2)
        self.assertIn("no snapshot", self.stderr)


class SeenJobsBase(DriveSyncCase):
    """`rows seen_jobs --base`: entries the session did not change keep another writer's edits."""

    @staticmethod
    def stored(key: str, entry: dict) -> list:
        return [cell[1:] for cell in ds.seen_row(key, entry)]  # as the Sheet holds a written row

    def scenario(self):
        entries = seen_entries()
        sheet = self.load_tab("seen_jobs", json.dumps({"seen": entries}))
        base = self.scratch / "base_snapshot_seen_jobs.json"
        base.write_text(self.read(".drive_sync/snapshot_seen_jobs.json"), encoding="utf-8")
        # The session ranks two jobs and finds two new ones (one another writer also found).
        entries["linkedin-1002"].update(status="ranked", rank_score=71)
        entries["board-77"]["status"] = "ranked"
        entries["board-80"] = {"title": "New", "company": "Hooli", "url": "https://example.com/80",
                               "first_seen": "2026-11-09", "status": "new", "deadline": None}
        entries["board-91"] = {"title": "Ours", "company": "Vandelay", "url": "https://example.com/91",
                               "first_seen": "2026-11-09", "status": "ranked", "deadline": None}
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        # Meanwhile another writer edits linkedin-1001 and board-77 and appends board-91 and board-92.
        self.theirs = dict(seen_entries()["linkedin-1001"], status="applied")
        sheet.rows[1] = self.stored("linkedin-1001", self.theirs)
        sheet.rows[3] = self.stored("board-77", dict(seen_entries()["board-77"], status="archived"))
        sheet.rows.append(self.stored("board-91", {"title": "Theirs", "company": "Vandelay",
                                                   "url": "https://example.com/91", "status": "new"}))
        self.board_92 = {"title": "Other", "company": "Wayne", "url": "https://example.com/92", "status": "new"}
        sheet.rows.append(self.stored("board-92", self.board_92))
        # The refresh before a write: export to scratch, so the snapshot is the Sheet as it is now.
        self.export("seen_jobs", sheet, "--out", self.scratch / "fresh.json", tag="fresh")
        return sheet, base, entries

    def test_session_changes_win_and_untouched_entries_keep_theirs(self):
        sheet, base, entries = self.scenario()
        plan = self.run_tool("rows", "seen_jobs", "--base", base)
        self.assertEqual(plan["counts"], {"append": 1, "update": 3, "unchanged": 2, "missing_locally": 1,
                                          "kept_theirs": 1})
        self.assertEqual([(u["key"], u["range"]) for u in plan["update"]], [
            ("linkedin-1002", "seen_jobs!A3:M3"),  # changed by the session only
            ("board-77", "seen_jobs!A4:M4"),       # changed by both: the session changed it, so it writes
            ("board-91", "seen_jobs!A7:M7"),       # not in the base, differs from the Sheet now
        ])
        self.assertEqual([row[0] for chunk in plan["append"] for row in chunk["values"]], ["'board-80"])
        self.assertIn("linkedin-1001", self.stderr)
        sheet.apply(plan)
        self.export("seen_jobs", sheet, "--out", self.scratch / "after.json", tag="after")
        after = json.loads((self.scratch / "after.json").read_text(encoding="utf-8"))["seen"]
        self.assertEqual(after["linkedin-1001"], self.theirs)
        self.assertEqual(after["board-92"], self.board_92)
        for key in ("linkedin-1002", "board-77", "board-80", "board-91", "board-78", "board-79"):
            self.assertEqual(after[key], entries[key], key)
        again = self.run_tool("rows", "seen_jobs", "--base", base)
        self.assertEqual(again["counts"], {"append": 0, "update": 0, "unchanged": 6, "missing_locally": 1,
                                           "kept_theirs": 1})

    def test_without_base_the_local_file_wins_as_before(self):
        _, _, _ = self.scenario()
        plan = self.run_tool("rows", "seen_jobs")
        self.assertEqual(plan["counts"], {"append": 1, "update": 4, "unchanged": 2, "missing_locally": 1})
        self.assertEqual([u["key"] for u in plan["update"]], ["linkedin-1001", "linkedin-1002", "board-77", "board-91"])

    def test_base_equal_to_the_current_snapshot_changes_nothing(self):
        entries = seen_entries()
        self.load_tab("seen_jobs", json.dumps({"seen": entries}))
        base = self.scratch / "base.json"
        base.write_text(self.read(".drive_sync/snapshot_seen_jobs.json"), encoding="utf-8")
        entries["board-79"]["status"] = "skipped"
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        self.assertEqual(self.run_tool("rows", "seen_jobs", "--base", base)["counts"],
                         {"append": 0, "update": 1, "unchanged": 4, "missing_locally": 0, "kept_theirs": 0})

    def test_bad_base_arguments(self):
        entries = seen_entries()
        self.load_tab("seen_jobs", json.dumps({"seen": entries}))
        self.run_tool("rows", "seen_jobs", "--base", self.scratch / "missing.json", expect=1)
        self.assertIn("not found", self.stderr)
        self.run_tool("rows", "seen_jobs", "--base", self.scratch_file("junk.json", '{"rows": [{"row": 2}]}'), expect=2)
        self.assertIn("not a seen_jobs snapshot", self.stderr)
        ledger_snap = self.scratch_file("ledger_snap.json", json.dumps({"tab": "ledger", "rows": []}))
        self.run_tool("rows", "seen_jobs", "--base", ledger_snap, expect=1)
        self.assertIn("'ledger' tab", self.stderr)
        base = self.scratch_file("base.json", self.read(".drive_sync/snapshot_seen_jobs.json"))
        self.write("job_search_tracker.csv", csv_text(tracker_rows()))
        self.run_tool("rows", "tracker", "--base", base, expect=1)
        self.assertIn("seen_jobs tab only", self.stderr)
        (self.root / ".drive_sync" / "snapshot_seen_jobs.json").unlink()
        self.run_tool("rows", "seen_jobs", "--base", base, expect=1)
        self.assertIn("export the seen_jobs tab again", self.stderr)


class LedgerValidation(DriveSyncCase):
    def export_ledger(self, rows, expect):
        sheet = FakeSheet(self, "ledger")
        sheet.rows = [list(ds.LEDGER_HEADER)] + rows
        return self.export("ledger", sheet, expect=expect)

    def good(self):
        return [ds.ledger_cells(r) for r in ledger_rows()]

    def test_a_valid_ledger_with_fill_runs_exports(self):
        self.export_ledger(self.good(), expect=0)

    def test_each_kind_of_slip_is_named_by_row(self):
        rows = self.good()
        rows[1][3] = "lane"                      # row 3: phase
        rows[2][4] = "exploded"                  # row 4: event
        rows[3][6] = '{"nonce": "a1b2c3"'        # row 5: detail_json
        rows[4][0] = "yesterday"                 # row 6: at
        rows[7][2] = "../escape"                 # row 9: key
        self.export_ledger(rows, expect=2)
        for needle in ("row 3: phase 'lane'", "row 4: event 'exploded'", "row 5: detail_json does not parse",
                       "row 6: at 'yesterday'", "row 9: key '../escape'"):
            self.assertIn(needle, self.stderr)
        self.assertFalse((self.root / "pipeline/ledger.json").exists())

    def test_a_job_row_needs_an_earlier_selected_row(self):
        rows = self.good()
        rows[1], rows[2] = rows[2], rows[1]  # fired before selected
        self.export_ledger(rows, expect=2)
        self.assertIn("row 3: no selected row for (2026-11-09, acme-ml-engineer)", self.stderr)

    def test_event_phase_and_run_key_rules(self):
        rows = self.good()
        rows[1][3] = "browser"                   # selected is a documents event
        rows[0][2] = "acme-ml-engineer"          # run rows carry an empty key
        rows[5][1] = "Nov 9"                     # run id
        self.export_ledger(rows, expect=2)
        self.assertIn("row 3: event selected belongs to phase documents, not browser", self.stderr)
        self.assertIn("row 2: run rows have an empty key", self.stderr)
        self.assertIn("row 7: run_id 'Nov 9'", self.stderr)

    def test_detail_json_must_be_an_object(self):
        rows = self.good()
        rows[2][6] = "[]"
        self.export_ledger(rows, expect=2)
        self.assertIn("row 4: detail is not a JSON object", self.stderr)

    def test_rows_validates_local_rows_before_writing(self):
        rows = ledger_rows()
        rows[2]["phase"] = "lane"
        self.write("pipeline/ledger.json", json.dumps({"rows": rows}))
        self.assertIsNone(self.run_tool("rows", "ledger", expect=2))
        self.assertIn("local row 3: phase 'lane'", self.stderr)


class Recovery(DriveSyncCase):
    """A push that only partly landed, or two sessions writing the same tab, never locks a tab:
    the next export still succeeds and the next `rows` writes only what is missing."""

    def test_a_partly_failed_seen_jobs_push_is_rewritten_without_duplicates(self):
        entries = {f"k{i}": {"title": f"t{i}"} for i in range(5)}
        sheet = self.load_tab("seen_jobs", json.dumps({"seen": entries}))
        entries.update({f"k{i}": {"title": f"t{i}"} for i in range(5, 65)})
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        plan = self.run_tool("rows", "seen_jobs")
        self.assertEqual([len(c["values"]) for c in plan["append"]], [25, 25, 10])
        del plan["append"][1]  # the second append_values call failed; the others landed
        sheet.apply(plan)
        before = self.read("job_scraper/seen_jobs.json")
        out = self.export("seen_jobs", sheet, "--verify-against", "job_scraper/seen_jobs.json", expect=2, tag="v1")
        self.assertEqual(len(out["differences"]), 25)
        self.assertEqual(self.read("job_scraper/seen_jobs.json"), before)  # the local file is never touched
        self.assertIn("rows seen_jobs", self.stderr)
        again = self.run_tool("rows", "seen_jobs")  # against the snapshot the mismatch recorded
        self.assertEqual(again["counts"], {"append": 25, "update": 0, "unchanged": 40, "missing_locally": 0})
        self.assertEqual([r[0] for c in again["append"] for r in c["values"]], [f"'k{i}" for i in range(30, 55)])
        sheet.apply(again)
        self.assertTrue(self.export("seen_jobs", sheet, "--verify-against", "job_scraper/seen_jobs.json", tag="v2")["match"])
        self.assertEqual(len({row[0] for row in sheet.rows}), len(sheet.rows))  # no key twice

    def test_a_key_repeated_with_the_same_entry_is_kept_once_and_updated_everywhere(self):
        entries = seen_entries()
        sheet = self.load_tab("seen_jobs", json.dumps({"seen": entries}))
        sheet.rows.append(list(sheet.rows[2]))  # two sessions appended linkedin-1002
        out = self.export("seen_jobs", sheet, tag="dup")
        self.assertEqual(out["duplicates"], [{"key": "linkedin-1002", "row": 7, "first_row": 3}])
        self.assertIn("row 7 (linkedin-1002) = row 3", self.stderr)
        self.assertEqual(list(json.loads(self.read("job_scraper/seen_jobs.json"))["seen"]), list(entries))
        entries["linkedin-1002"]["status"] = "ranked"
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        plan = self.run_tool("rows", "seen_jobs")
        self.assertEqual([u["range"] for u in plan["update"]], ["seen_jobs!A3:M3", "seen_jobs!A7:M7"])
        sheet.apply(plan)
        verified = self.export("seen_jobs", sheet, "--verify-against", "job_scraper/seen_jobs.json", tag="v")
        self.assertTrue(verified["match"])

    def test_a_key_repeated_with_a_different_entry_is_a_conflict(self):
        sheet = self.load_tab("seen_jobs", json.dumps({"seen": seen_entries()}))
        sheet.rows.append(list(sheet.rows[2]))
        sheet.rows[-1][-1] = sheet.rows[-1][-1].replace('"new"', '"ranked"')
        self.export("seen_jobs", sheet, expect=2, tag="conflict")
        self.assertIn("row 7: key linkedin-1002 repeats row 3 with a different entry_json", self.stderr)

    def test_unpushed_ledger_rows_stop_a_plain_export_and_are_appended_once(self):
        rows = ledger_rows()
        sheet = self.load_tab("ledger", json.dumps({"rows": rows[:2]}))
        local = json.loads(self.read("pipeline/ledger.json"))
        local["rows"] += rows[2:5]
        self.write("pipeline/ledger.json", json.dumps(local))
        plan = self.run_tool("rows", "ledger", "--chunk", "1")
        self.assertEqual(len(plan["append"]), 3)
        del plan["append"][1]  # the append of the `claimed` row failed
        sheet.apply(plan)
        before = self.read("pipeline/ledger.json")
        out = self.export("ledger", sheet, expect=2, tag="after")
        self.assertEqual([(u["position"], u["event"]) for u in out["unpushed"]], [(4, "claimed")])
        self.assertIn("local row 4 (claimed 2026-11-09 acme-ml-engineer)", self.stderr)
        self.assertIn("--discard-local", self.stderr)
        self.assertEqual(self.read("pipeline/ledger.json"), before)  # nothing lost
        again = self.run_tool("rows", "ledger")  # rows that landed are recognised by content
        self.assertEqual(again["counts"], {"append": 1, "update": 0, "unchanged": 4, "missing_locally": 0})
        self.assertEqual(again["append"][0]["values"][0][4], "'claimed")
        self.assertIn("already in the Sheet", self.stderr)
        sheet.apply(again)
        self.export("ledger", sheet, tag="final")
        exported = json.loads(self.read("pipeline/ledger.json"))["rows"]
        self.assertEqual(len(exported), 5)
        self.assertTrue(all(isinstance(r["row"], int) for r in exported))

    def test_discard_local_drops_unpushed_ledger_rows(self):
        rows = ledger_rows()
        sheet = self.load_tab("ledger", json.dumps({"rows": rows[:2]}))
        local = json.loads(self.read("pipeline/ledger.json"))
        local["rows"].append(rows[2])
        self.write("pipeline/ledger.json", json.dumps(local))
        out = self.export("ledger", sheet, "--discard-local", tag="drop")
        self.assertEqual(out["discarded_local"], 1)
        self.assertEqual(len(json.loads(self.read("pipeline/ledger.json"))["rows"]), 2)

    def test_a_plain_export_reports_local_seen_keys_and_tracker_rows_it_drops(self):
        entries = seen_entries()
        sheet = self.load_tab("seen_jobs", json.dumps({"seen": entries}))
        entries["board-90"] = {"title": "never pushed"}
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": entries}))
        self.assertEqual(self.export("seen_jobs", sheet, tag="again")["dropped_local"], 1)
        self.assertIn("board-90", self.stderr)
        rows = tracker_rows()
        tracker = self.load_tab("tracker", csv_text(rows))
        self.write("job_search_tracker.csv", csv_text(rows + [rows[0]]))
        self.assertEqual(self.export("tracker", tracker, tag="again")["dropped_local"], 1)
        self.assertIn("past the Sheet's last row", self.stderr)

    def test_rows_refuses_an_empty_seen_jobs_key(self):
        self.write("job_scraper/seen_jobs.json", json.dumps({"seen": {"good-1": {"title": "a"}, "": {"title": "b"}}}))
        self.assertIsNone(self.run_tool("rows", "seen_jobs", expect=2))
        self.assertIn("must not be empty", self.stderr)

    def test_rows_names_a_lone_surrogate_instead_of_crashing(self):
        text = '{"seen": {"good-1": {"title": "a"}, "bad-2": {"title": "cut emoji \\ud83d"}}}'
        self.write("job_scraper/seen_jobs.json", text)
        self.assertIsNone(self.run_tool("rows", "seen_jobs", expect=2))
        self.assertIn("seen_jobs key bad-2: column title holds text that is not valid UTF-8", self.stderr)
        self.assertNotIn("Traceback", self.stderr)
        self.load_tab("seen_jobs", json.dumps({"seen": {"good-1": {"title": "a"}}}))
        self.write("job_scraper/seen_jobs.json", text)  # with a snapshot, where the entry is hashed
        self.run_tool("rows", "seen_jobs", expect=2)
        self.assertIn("seen_jobs key bad-2: column", self.stderr)
        rows = ledger_rows()[:2]
        rows[1]["detail"]["title"] = "\ud83d"
        self.write("pipeline/ledger.json", json.dumps({"rows": rows}))
        self.run_tool("rows", "ledger", expect=2)
        self.assertIn("ledger local row 2: column detail_json holds text that is not valid UTF-8", self.stderr)
        self.assertNotIn("Traceback", self.stderr)


class DecodeAndVerify(DriveSyncCase):
    TEXT ="\\documentclass{moderncv}\n% Zürich #1 \n\\begin{document}\nHello\n\\end{document}\n"

    def test_saved_result_is_decoded_verified_and_recorded(self):
        out = self.pull("cv/main_acme_ml_engineer.tex", self.TEXT, "FILE_ID_CV1")
        self.assertEqual(out, {"path": "cv/main_acme_ml_engineer.tex", "sha256": sha(self.TEXT),
                               "size": len(self.TEXT.encode("utf-8")), "verified": True})
        self.assertEqual((self.root / "cv/main_acme_ml_engineer.tex").read_bytes(), self.TEXT.encode("utf-8"))
        pulled = json.loads(self.read(".drive_sync/pulled.json"))
        self.assertEqual(pulled["cv/main_acme_ml_engineer.tex"],
                         {"id": "FILE_ID_CV1", "sha256": sha(self.TEXT), "size": len(self.TEXT.encode("utf-8"))})

    def test_saved_result_wrapped_in_content_blocks(self):
        saved = self.tool_output("wrapped", self.TEXT, "FILE_ID_W", wrap=True)
        out = self.run_tool("decode", "--tool-output", saved, "--out", "documents/applications/acme/job_posting.md")
        self.assertIsNone(out["verified"])
        pulled = json.loads(self.read(".drive_sync/pulled.json"))
        self.assertEqual(pulled["documents/applications/acme/job_posting.md"]["id"], "FILE_ID_W")

    def test_inline_copy_with_line_breaks_is_decoded(self):
        wrapped = "\n".join("  " + line for line in textwrap.wrap(b64(self.TEXT), 20))
        copy = self.scratch_file("inline.b64", wrapped + "\n")
        out = self.run_tool("decode", "--b64-file", copy, "--out", "pipeline/config.yaml.copy",
                            "--id", "FILE_ID_CFG", "--expect-sha256", sha(self.TEXT))
        self.assertTrue(out["verified"])
        self.assertEqual(self.read("pipeline/config.yaml.copy"), self.TEXT)

    def test_inline_copy_missing_its_padding_still_decodes(self):
        copy = self.scratch_file("inline.b64", b64("ab").rstrip("="))
        out = self.run_tool("decode", "--b64-file", copy, "--out", "x.txt", "--expect-sha256", sha("ab"))
        self.assertTrue(out["verified"])

    def test_without_a_checksum_a_copy_that_lost_a_character_is_refused(self):
        dropped = b64(self.TEXT)[:-6] + b64(self.TEXT)[-5:]  # one character lost mid-copy
        copy = self.scratch_file("inline.b64", dropped)
        self.run_tool("decode", "--b64-file", copy, "--out", "cv/main_hand_uploaded.tex", expect=2)
        self.assertIn("not a multiple of 4", self.stderr)
        self.assertFalse((self.root / "cv/main_hand_uploaded.tex").exists())
        self.assertFalse((self.root / ".drive_sync/pulled.json").exists())

    def test_without_a_checksum_the_download_is_marked_not_verified(self):
        copy = self.scratch_file("inline.b64", b64(self.TEXT))
        out = self.run_tool("decode", "--b64-file", copy, "--out", "CLAUDE.md")
        self.assertIsNone(out["verified"])
        self.assertIn("is not verified", self.stderr)

    def test_checksum_mismatch_writes_nothing(self):
        copy = self.scratch_file("inline.b64", b64(self.TEXT.replace("Hello", "Hallo")))
        out = self.run_tool("decode", "--b64-file", copy, "--out", "cv/main_x.tex", "--id", "FILE_ID_X",
                            "--description", f"sha256={sha(self.TEXT)} size={len(self.TEXT.encode('utf-8'))}",
                            expect=2)
        self.assertFalse(out["verified"])
        self.assertIn("checksum mismatch", self.stderr)
        self.assertFalse((self.root / "cv/main_x.tex").exists())
        self.assertFalse((self.root / ".drive_sync/pulled.json").exists())

    def test_size_mismatch_alone_is_a_mismatch(self):
        copy = self.scratch_file("inline.b64", b64(self.TEXT))
        self.run_tool("decode", "--b64-file", copy, "--out", "y.txt",
                      "--description", f"sha256={sha(self.TEXT)} size=3", expect=2)

    def test_a_result_for_another_file_id_is_refused(self):
        saved = self.tool_output("other", self.TEXT, "FILE_ID_OTHER")
        self.run_tool("decode", "--tool-output", saved, "--out", "z.txt", "--id", "FILE_ID_WANTED", expect=2)
        self.assertFalse((self.root / "z.txt").exists())

    def test_a_description_without_a_checksum_is_not_verified(self):
        copy = self.scratch_file("inline.b64", b64(self.TEXT))
        out = self.run_tool("decode", "--b64-file", copy, "--out", "w.txt", "--description", "uploaded by hand")
        self.assertIsNone(out["verified"])
        self.assertIn("not verified", self.stderr)

    def test_a_saved_result_holding_bare_base64(self):
        saved = self.scratch_file("bare.txt", b64(self.TEXT) + "\n")
        out = self.run_tool("decode", "--tool-output", saved, "--out", "u.txt", "--expect-sha256", sha(self.TEXT))
        self.assertTrue(out["verified"])

    def test_bad_base64_is_a_data_error(self):
        copy = self.scratch_file("bad.b64", "this is not base64!")
        self.run_tool("decode", "--b64-file", copy, "--out", "v.txt", expect=2)

    def test_describe(self):
        self.write("pipeline/runs/2026-11-09/report.md", self.TEXT)
        out = self.run_tool("describe", "--local", "pipeline/runs/2026-11-09/report.md")
        size = len(self.TEXT.encode("utf-8"))
        self.assertEqual(out, {"path": "pipeline/runs/2026-11-09/report.md", "sha256": sha(self.TEXT),
                               "size": size, "description": f"sha256={sha(self.TEXT)} size={size}"})

    def test_verify_matches_saved_and_inline_copies(self):
        self.write("cv/main_acme.tex", self.TEXT)
        saved = self.tool_output("saved", self.TEXT, "FILE_ID_S")
        out = self.run_tool("verify", "--local", "cv/main_acme.tex", "--tool-output", saved)
        self.assertTrue(out["match"])
        self.assertEqual(out["remote"]["id"], "FILE_ID_S")
        inline = self.scratch_file("inline.b64", b64(self.TEXT))
        self.assertTrue(self.run_tool("verify", "--local", "cv/main_acme.tex", "--b64-file", inline)["match"])

    def test_verify_mismatch_names_the_first_differing_line(self):
        self.write("cv/main_acme.tex", self.TEXT)
        inline = self.scratch_file("inline.b64", b64(self.TEXT.replace("% Zürich #1 ", "% Zurich #1")))
        out = self.run_tool("verify", "--local", "cv/main_acme.tex", "--b64-file", inline, expect=2)
        self.assertFalse(out["match"])
        self.assertEqual(out["first_diff_line"], 2)
        self.assertNotEqual(out["local"]["sha256"], out["remote"]["sha256"])
        self.assertIn("line 2", self.stderr)

    def test_verify_notices_a_dropped_final_newline(self):
        self.write("a.md", "one\ntwo\n")
        inline = self.scratch_file("inline.b64", b64("one\ntwo"))
        self.assertEqual(self.run_tool("verify", "--local", "a.md", "--b64-file", inline, expect=2)["first_diff_line"], 2)


class PushPlan(DriveSyncCase):
    def setUp(self):
        super().setUp()
        self.pull("CLAUDE.md", "# Profile\n", "FILE_ID_CLAUDE")
        self.pull("cv/main_example.tex", "% example\n", "FILE_ID_EXAMPLE")
        self.pull("job_scraper/notion_sync.json", '{"synced": {}}\n', "FILE_ID_NOTION")
        self.pull("company_research/acme-corp.json", '{"company": "Acme Corp"}\n', "FILE_ID_RESEARCH")

    def test_orchestrator_create_replace_and_refused(self):
        self.write("job_scraper/notion_sync.json", '{"synced": {"linkedin-1001": "page"}}\n')
        self.write("CLAUDE.md", "# Profile, edited\n")
        self.write(f"pipeline/runs/{RUN}/report.md", "# Report\n")
        self.write(f"pipeline/runs/{RUN}/stages/rank.md", "ranked\n")
        self.write(f"pipeline/runs/{RUN}/report.pdf", "not really a pdf")
        self.write(f"pipeline/runs/{RUN}/report.aux", "aux")
        (self.root / f"pipeline/runs/{RUN}/logo.bin").write_bytes(b"\xff\xfe\x00binary")
        self.write("pipeline/runs/2026-11-08/report.md", "an older run\n")
        out = self.run_tool("push-plan", "--role", "orchestrator", "--run-id", RUN)
        by_path = {f["path"]: f for f in out["files"]}
        self.assertEqual(sorted(by_path), ["job_scraper/notion_sync.json", f"pipeline/runs/{RUN}/report.md",
                                           f"pipeline/runs/{RUN}/stages/rank.md"])
        notion = by_path["job_scraper/notion_sync.json"]
        self.assertEqual((notion["action"], notion["replaces_id"]), ("replace", "FILE_ID_NOTION"))
        report = by_path[f"pipeline/runs/{RUN}/report.md"]
        self.assertEqual((report["action"], report["replaces_id"]), ("create", None))
        self.assertEqual((report["folder"], report["title"]), (f"pipeline/runs/{RUN}", "report.md"))
        self.assertEqual(report["description"], f"sha256={sha('# Report' + chr(10))} size=9")
        refused = {r["path"]: r["reason"] for r in out["refused"]}
        self.assertEqual(sorted(refused), ["CLAUDE.md", f"pipeline/runs/{RUN}/logo.bin"])
        self.assertIn("create-only", refused["CLAUDE.md"])
        self.assertIn("binary", refused[f"pipeline/runs/{RUN}/logo.bin"])

    def test_a_changed_overlay_file_is_refused_not_skipped(self):
        self.write("cv/main_example.tex", "% example, edited locally\n")
        out = self.run_tool("push-plan", "--role", "interactive")
        self.assertEqual(out["files"], [])
        self.assertEqual([r["path"] for r in out["refused"]], ["cv/main_example.tex"])

    def test_unchanged_pulled_files_are_not_pushed(self):
        out = self.run_tool("push-plan", "--role", "interactive")
        self.assertEqual(out["files"], [])
        self.assertEqual(out["refused"], [])

    def test_documents_lane_pushes_its_job_files_and_replaces_research(self):
        name = "acme_ml_engineer"
        for rel in (f"cv/main_{name}.tex", f"cover_letters/cover_{name}.tex",
                    f"documents/applications/{name}/job_posting.md",
                    f"documents/applications/{name}/application_form_answers.md",
                    f"pipeline/runs/{RUN}/lanes/acme-ml-engineer.json", f"pipeline/runs/{RUN}/lanes/acme-ml-engineer.md",
                    f"cv/main_{name}.log", f"documents/applications/{name}/notes.md", "cv/main_other_job.tex"):
            self.write(rel, f"content of {rel}\n")
        self.write("company_research/acme-corp.json", '{"company": "Acme Corp", "fetched_date": "2026-11-09"}\n')
        out = self.run_tool("push-plan", "--role", "documents-lane", "--run-id", RUN, "--key", "acme-ml-engineer",
                            "--company", "Acme Corp", "--job-folder", name)
        actions = {f["path"]: f["action"] for f in out["files"]}
        self.assertEqual(actions, {
            "company_research/acme-corp.json": "replace",
            f"cv/main_{name}.tex": "create", f"cover_letters/cover_{name}.tex": "create",
            f"documents/applications/{name}/job_posting.md": "create",
            f"documents/applications/{name}/application_form_answers.md": "create",
            f"pipeline/runs/{RUN}/lanes/acme-ml-engineer.json": "create",
            f"pipeline/runs/{RUN}/lanes/acme-ml-engineer.md": "create",
        })
        self.assertEqual(out["unresolved"], [])

    def test_a_changed_pulled_file_that_is_not_replaceable_is_refused(self):
        name = "acme_ml_engineer"
        self.pull(f"cv/main_{name}.tex", "% drafted\n", "FILE_ID_CV")
        self.write(f"cv/main_{name}.tex", "% edited after the pull\n")
        out = self.run_tool("push-plan", "--role", "documents-lane", "--run-id", RUN, "--key", "k1",
                            "--job-folder", name)
        self.assertEqual(out["files"], [])
        self.assertEqual(out["refused"], [{"path": f"cv/main_{name}.tex",
                                           "reason": "create-only: changed since it was pulled, and not replaceable"}])

    def test_replace_needs_drive_replaceable_too(self):
        self.write_config(replaceable=["company_research/*.json"])
        self.write("job_scraper/notion_sync.json", '{"synced": {"x": 1}}\n')
        out = self.run_tool("push-plan", "--role", "orchestrator", "--run-id", RUN)
        self.assertEqual(out["files"], [])
        self.assertEqual([r["path"] for r in out["refused"]], ["job_scraper/notion_sync.json"])

    def test_filler_and_browser_lane_push_only_their_own_files(self):
        self.write("pipeline/fills/fill-2026-11-10/report.md", "# Fill\n")
        self.write(f"pipeline/runs/{RUN}/lanes/k1.browser.json", "{}\n")
        self.write(f"pipeline/runs/{RUN}/lanes/k1.browser.md", "## Browser\n")
        self.write(f"pipeline/runs/{RUN}/lanes/k1.json", "{}\n")
        filler = self.run_tool("push-plan", "--role", "filler", "--fill-id", "fill-2026-11-10")
        self.assertEqual([f["path"] for f in filler["files"]], ["pipeline/fills/fill-2026-11-10/report.md"])
        browser = self.run_tool("push-plan", "--role", "browser-lane", "--run-id", RUN, "--key", "k1")
        self.assertEqual([f["path"] for f in browser["files"]],
                         [f"pipeline/runs/{RUN}/lanes/k1.browser.json", f"pipeline/runs/{RUN}/lanes/k1.browser.md"])

    def test_interactive_creates_anything_new_under_the_data_paths(self):
        self.write("documents/applications/globex_analyst/outcome.md", "rejected\n")
        self.write("cv/main_globex_analyst.tex", "% cv\n")
        self.write("cover_letters/cover_example.tex", "% framework file\n")
        self.write("tools/new_tool.py", "print('not data')\n")
        self.write("documents/applications/globex_analyst/.DS_Store", "junk")
        out = self.run_tool("push-plan", "--role", "interactive")
        self.assertEqual([f["path"] for f in out["files"]],
                         ["cv/main_globex_analyst.tex", "documents/applications/globex_analyst/outcome.md"])


class Paths(DriveSyncCase):
    BASE = ["pipeline/config.yaml", "CLAUDE.md", "cv/main_example.tex",
            ".claude/skills/job-application-assistant/01-candidate-profile.md"]

    def test_orchestrator(self):
        out = self.run_tool("paths", "--role", "orchestrator", "--run-id", RUN)
        self.assertEqual(out["role"], "orchestrator")
        self.assertEqual(out["pull"], {"files": self.BASE + ["job_scraper/notion_sync.json"], "globs": [],
                                       "tabs": ["seen_jobs", "tracker", "ledger"]})
        self.assertEqual(out["push"], {"create": [f"pipeline/runs/{RUN}/**"],
                                       "replace": ["job_scraper/notion_sync.json"],
                                       "tabs": {"seen_jobs": "append+update", "tracker": "append", "ledger": "append"}})
        self.assertEqual(out["unresolved"], [])

    def test_orchestrator_before_the_run_id_exists(self):
        out = self.run_tool("paths", "--role", "orchestrator")
        self.assertEqual(out["push"]["create"], ["pipeline/runs/*/**"])
        self.assertEqual(out["unresolved"], ["--run-id not given: push globs use * in its place"])

    def test_documents_lane_normalizes_the_company_like_the_research_cache(self):
        out = self.run_tool("paths", "--role", "documents-lane", "--run-id", RUN, "--key", "k1",
                            "--company", "  Acme Corp ", "--job-folder", "acme_ml_engineer")
        self.assertEqual(out["pull"], {"files": self.BASE + ["company_research/acme-corp.json"], "globs": [],
                                       "tabs": ["ledger"]})
        self.assertEqual(out["push"]["create"], [
            "cv/main_acme_ml_engineer.tex", "cover_letters/cover_acme_ml_engineer.tex",
            "documents/applications/acme_ml_engineer/job_posting.md",
            "documents/applications/acme_ml_engineer/application_form_answers.md",
            f"pipeline/runs/{RUN}/lanes/k1.json", f"pipeline/runs/{RUN}/lanes/k1.md"])
        self.assertEqual(out["push"]["replace"], ["company_research/*.json"])
        self.assertEqual(out["push"]["tabs"], {"ledger": "append"})

    def test_documents_lane_never_pulls_a_company_name_that_is_a_path(self):
        out = self.run_tool("paths", "--role", "documents-lane", "--company", "../../etc")
        self.assertEqual(out["pull"]["files"], self.BASE)
        self.assertTrue(any("single file name" in note for note in out["unresolved"]))

    def test_browser_lane_pulls_the_lane_result_and_the_files_it_names(self):
        lane = f"pipeline/runs/{RUN}/lanes/k1.json"
        first = self.run_tool("paths", "--role", "browser-lane", "--run-id", RUN, "--key", "k1")
        self.assertEqual(first["pull"]["files"], self.BASE + [lane])
        self.assertTrue(any("not in the working copy yet" in n for n in first["unresolved"]))
        self.write(lane, json.dumps({"status": "drafted", "files": {
            "cv": "cv/main_acme_ml_engineer.tex", "cover_letter": "cover_letters/cover_acme_ml_engineer.tex",
            "answers": "documents/applications/acme_ml_engineer/application_form_answers.md",
            "cv_pdf": "cv/main_acme_ml_engineer.pdf", "elsewhere": "/root/outputs/cv.pdf", "tool": "tools/x.py"}}))
        out = self.run_tool("paths", "--role", "browser-lane", "--run-id", RUN, "--key", "k1")
        self.assertEqual(out["pull"]["files"], self.BASE + [
            lane, "cv/main_acme_ml_engineer.tex", "cover_letters/cover_acme_ml_engineer.tex",
            "documents/applications/acme_ml_engineer/application_form_answers.md"])
        self.assertEqual(out["pull"]["tabs"], ["ledger"])
        self.assertEqual(out["push"]["create"], [f"pipeline/runs/{RUN}/lanes/k1.browser.json",
                                                 f"pipeline/runs/{RUN}/lanes/k1.browser.md"])
        self.assertEqual(out["push"]["replace"], [])
        self.assertTrue(any("'/root/outputs/cv.pdf'" in n for n in out["unresolved"]))
        self.assertTrue(any("'tools/x.py'" in n for n in out["unresolved"]))

    def test_browser_lane_with_a_job_folder(self):
        out = self.run_tool("paths", "--role", "browser-lane", "--run-id", RUN, "--key", "k1",
                            "--job-folder", "acme_ml_engineer")
        self.assertIn("cv/main_acme_ml_engineer.tex", out["pull"]["files"])
        self.assertIn("documents/applications/acme_ml_engineer/application_form_answers.md", out["pull"]["files"])

    def test_filler(self):
        out = self.run_tool("paths", "--role", "filler", "--fill-id", "fill-2026-11-10")
        # It reads the tracker (fill-queue holds jobs already applied for) but writes only the ledger.
        self.assertEqual(out["pull"], {"files": self.BASE, "globs": [], "tabs": ["ledger", "tracker"]})
        self.assertEqual(out["push"], {"create": ["pipeline/fills/fill-2026-11-10/**"], "replace": [],
                                       "tabs": {"ledger": "append"}})

    def test_company_file_is_read_literally_and_normalized_like_company(self):
        name = "  Acme $(touch pwned) `id` O'Brien & Co\n"
        stripped = name.strip()
        path = self.scratch_file("company.txt", name)
        common = ("--role", "documents-lane", "--run-id", RUN, "--key", "k1", "--job-folder", "acme_ml_engineer")
        out = self.run_tool("paths", *common, "--company-file", path)
        self.assertEqual(out, self.run_tool("paths", *common, "--company", stripped))
        self.assertEqual(out["pull"]["files"],
                         self.BASE + ["company_research/acme-$(touch-pwned)-`id`-o'brien-&-co.json"])
        self.assertFalse((self.root / "pwned").exists())
        self.assertFalse((self.scratch / "pwned").exists())
        # Relative to the root, with a byte order mark, through push-plan as well.
        self.write("notes/company.txt", "\ufeff Globex Labs \r\n")
        out = self.run_tool("paths", *common, "--company-file", "notes/company.txt")
        self.assertIn("company_research/globex-labs.json", out["pull"]["files"])
        plan = self.run_tool("push-plan", *common, "--company-file", path)
        self.assertEqual(plan["files"], [])

    def test_company_file_that_is_a_path_is_not_pulled(self):
        path = self.scratch_file("company.txt", "../../etc\n")
        out = self.run_tool("paths", "--role", "documents-lane", "--company-file", path)
        self.assertEqual(out["pull"]["files"], self.BASE)
        self.assertTrue(any("single file name" in note for note in out["unresolved"]))

    def test_company_and_company_file_together_are_a_usage_error(self):
        path = self.scratch_file("company.txt", "Acme Corp\n")
        for command in ("paths", "push-plan"):
            self.assertIsNone(self.run_tool(command, "--role", "documents-lane", "--company", "Acme Corp",
                                            "--company-file", path, expect=1))
        self.assertIn("--company", self.stderr)

    def test_an_empty_missing_or_undecodable_company_file_is_a_usage_error(self):
        self.run_tool("paths", "--role", "documents-lane", "--company-file", self.scratch_file("blank.txt", " \n\t\n"),
                      expect=1)
        self.assertIn("is empty", self.stderr)
        self.run_tool("paths", "--role", "documents-lane", "--company-file", self.scratch / "nope.txt", expect=1)
        self.assertIn("not found", self.stderr)
        latin = self.scratch / "latin.txt"
        latin.write_bytes("Société Générale".encode("latin-1"))
        self.run_tool("paths", "--role", "documents-lane", "--company-file", latin, expect=1)
        self.assertIn("not UTF-8", self.stderr)

    def test_interactive(self):
        out = self.run_tool("paths", "--role", "interactive")
        self.assertEqual(out["pull"]["tabs"], ["seen_jobs", "tracker", "ledger"])
        self.assertIn("job_scraper/notion_sync.json", out["pull"]["files"])
        self.assertIn("documents/applications/**", out["pull"]["globs"])
        self.assertIn("pipeline/runs/**", out["push"]["create"])
        self.assertEqual(out["push"]["replace"], ["job_scraper/notion_sync.json", "company_research/*.json"])
        self.assertEqual(out["push"]["tabs"], {"seen_jobs": "append+update", "tracker": "append+update",
                                               "ledger": "append"})

    def test_unsafe_selectors_are_usage_errors(self):
        self.run_tool("paths", "--role", "orchestrator", "--run-id", "../2026-11-09", expect=1)
        self.run_tool("paths", "--role", "browser-lane", "--key", "a/b", expect=1)
        self.run_tool("paths", "--role", "documents-lane", "--job-folder", "x/../y", expect=1)
        self.run_tool("paths", "--role", "nobody", expect=1)

    def test_missing_or_broken_config_is_a_usage_error(self):
        (self.root / "pipeline/config.yaml").unlink()
        self.run_tool("paths", "--role", "filler", expect=1)
        self.assertIn("pull it from Drive first", self.stderr)
        self.write("pipeline/config.yaml", json.dumps({"drive": {"overlay": ["/abs/path"]}}))
        self.run_tool("paths", "--role", "filler", expect=1)
        self.assertIn("not a relative path", self.stderr)

    def test_root_may_follow_the_command(self):
        out = self.run_tool("paths", "--role", "filler", root_first=False)
        self.assertEqual(out["role"], "filler")


class CollisionAndIds(DriveSyncCase):
    def test_collision_lookups(self):
        out = self.run_tool("collision", "--name", "acme_ml_engineer")
        self.assertEqual(out["lookups"], [
            {"folder": "cv", "title": "main_acme_ml_engineer.tex", "kind": "file"},
            {"folder": "cover_letters", "title": "cover_acme_ml_engineer.tex", "kind": "file"},
            {"folder": "documents/applications", "title": "acme_ml_engineer", "kind": "folder"},
        ])
        self.run_tool("collision", "--name", "acme/ml", expect=1)

    def test_ids_cache_and_config_fallbacks(self):
        self.assertEqual(self.run_tool("ids", "get", "."), {"path": "", "id": "FOLDER_ID_ROOT", "source": "config"})
        self.assertEqual(self.run_tool("ids", "get", "cv/"), {"path": "cv", "id": "FOLDER_ID_CV", "source": "config"})
        self.assertEqual(self.run_tool("ids", "get", "pipeline/runs"),
                         {"path": "pipeline/runs", "id": None, "source": None})
        self.assertEqual(self.run_tool("ids", "set", "pipeline/runs", "FOLDER_ID_RUNS"),
                         {"path": "pipeline/runs", "id": "FOLDER_ID_RUNS"})
        self.assertEqual(self.run_tool("ids", "get", "./pipeline/runs"),
                         {"path": "pipeline/runs", "id": "FOLDER_ID_RUNS", "source": "cache"})
        self.assertEqual(json.loads(self.read(".drive_sync/ids.json")), {"pipeline/runs": "FOLDER_ID_RUNS"})
        self.run_tool("ids", "set", "../outside", "FOLDER_ID_X", expect=1)
        self.run_tool("ids", "set", "cv", "not an id", expect=1)


# ---------------------------------------------------------------------------
# Integration: drive_sync.py and pipeline_state.py through a whole night and morning

STATE_TOOL = REPO / "pipeline" / "pipeline_state.py"
NIGHT = "2026-11-09"


def pacific(clock: str, day: str = NIGHT) -> str:
    """A time on the night of the walk (Pacific standard time), for pipeline_state's --now."""
    return f"{day}T{clock}:00-08:00"


def without_row(row: dict) -> dict:
    return {k: v for k, v in row.items() if k != "row"}


def csv_body(path: Path) -> list:
    """A tracker CSV as (header, padded rows), read independently of either tool."""
    with path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    header = rows[0]
    return [header] + [r + [""] * (len(header) - len(r)) for r in rows[1:] if any(r)]


def rank_state_dump(doc) -> str:
    """tools/rank_state.py save_state's format, which /scrape and /rank write."""
    return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"


class IntegrationFakeSheet:
    """The FakeSheet helper of IntegrationTest: the whole job_search_data spreadsheet, as Sheets
    keeps it. Writes are USER_ENTERED, so every cell must arrive as "'" + text and is stored as
    that text (the apostrophe is not returned on read). append_values lands after the last row
    that holds data; update_values rewrites one row in place; row numbers never move. get_values
    returns display strings, omits trailing empty cells and trailing empty rows, and answers with
    the requested range ({"range": "<tab>!A<first>:<col><last>", "values": [...]}), with no
    "values" at all when the range holds nothing."""

    def __init__(self, test: unittest.TestCase):
        self.test = test
        self.tabs = {tab: [] for tab in ds.TABS}  # tabs[tab][n - 1] is Sheet row n
        self.saved = 0

    def entered(self, cell) -> str:
        self.test.assertIsInstance(cell, str)
        self.test.assertTrue(cell.startswith("'"), f"a cell written without its apostrophe: {cell!r}")
        return cell[1:]

    def append_values(self, a1: str, values: list) -> None:
        tab = a1.partition("!")[0]
        self.test.assertEqual(a1, f"{tab}!A1")
        rows = self.tabs[tab]
        while rows and not any(rows[-1]):
            rows.pop()
        for row in values:
            self.test.assertEqual(len(row), len(ds.HEADERS[tab]), f"{tab}: appended a short row {row}")
            rows.append([self.entered(c) for c in row])

    def update_values(self, a1: str, values: list) -> None:
        m = re.fullmatch(r"(\w+)!A(\d+):([A-Z]+)(\d+)", a1)
        self.test.assertIsNotNone(m, a1)
        tab, number = m.group(1), int(m.group(2))
        self.test.assertEqual(int(m.group(4)), number, a1)
        self.test.assertEqual(m.group(3), ds.col_letter(len(ds.HEADERS[tab])), a1)
        self.test.assertTrue(1 < number <= len(self.tabs[tab]), f"update outside the tab's data rows: {a1}")
        self.test.assertEqual(len(values), 1, a1)
        self.tabs[tab][number - 1] = [self.entered(c) for c in values[0]]

    def apply(self, plan: dict) -> None:
        """append_values for each append chunk, update_values for each update, as printed."""
        for chunk in plan["append"]:
            self.append_values(chunk["range"], chunk["values"])
        for update in plan["update"]:
            self.update_values(update["range"], update["values"])

    def get_values(self, a1: str) -> dict:
        m = re.fullmatch(r"(\w+)!A(\d+):([A-Z]+)(\d+)", a1)
        tab, first, col, last = m.group(1), int(m.group(2)), m.group(3), int(m.group(4))
        values = []
        for row in self.tabs[tab][first - 1:last]:
            row = list(row[:ds.col_number(col)])
            while row and row[-1] == "":
                row.pop()
            values.append(row)
        while values and not values[-1]:
            values.pop()
        result = {"range": f"{tab}!A{first}:{col}{last}", "majorDimension": "ROWS"}
        if values:
            result["values"] = values
        return result

    def save_pages(self, tab: str, directory: Path, size: int) -> list:
        """drive-sync's export procedure: get_values pages of `size` rows, each saved as a tool
        result file, until a page comes back with fewer rows than asked for (or none)."""
        col = ds.col_letter(len(ds.HEADERS[tab]))
        files, first = [], 1
        while True:
            result = self.get_values(f"{tab}!A{first}:{col}{first + size - 1}")
            self.saved += 1
            path = directory / f"get_values_{self.saved:04d}_{tab}.json"
            path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
            files.append(path)
            if len(result.get("values", [])) < size:
                return files
            first += size


class IntegrationTest(unittest.TestCase):
    """drive_sync.py and pipeline_state.py were built by different hands from one contract
    (DESIGN-core.md). This walks both CLIs through a whole night and morning in one working copy,
    with an IntegrationFakeSheet standing in for Google Sheets: the first load into empty tabs,
    the nightly run (new-run, check-run, select, lanes, documents lanes with a duplicate claim,
    record, finish-run) and the morning fill (fill-start, fill-queue, the browser lane,
    fill-finish, stats). After every push the tab is exported again and must equal what
    pipeline_state.py wrote, drive_sync.py's ledger validation must accept every row
    pipeline_state.py produced, and seen_jobs must survive the round trip exactly."""

    LEDGER_PAGE = 7  # small ledger pages, so its exports always span several get_values pages

    def setUp(self):
        try:
            import yaml
        except ImportError:  # pragma: no cover - the repo's tools need PyYAML anyway
            self.skipTest("PyYAML is needed to read pipeline/config.example.yaml")
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.base = Path(self._tmp.name)
        self.root = self.base / "work"
        self.scratch = self.base / "scratch"
        self.root.mkdir()
        self.scratch.mkdir()
        self.sheet = IntegrationFakeSheet(self)

        cfg = yaml.safe_load((REPO / "pipeline" / "config.example.yaml").read_text(encoding="utf-8"))
        cfg["drive"].update(root_id="FOLDER_ID_ROOT", sheet_id="SHEET_ID_TEST")
        cfg["lanes"].update(documents_task_id="TASK_ID_DOCUMENTS", browser_task_id="TASK_ID_BROWSER")
        cfg["stage_defs"]["autofill"]["chrome_device_id"] = "CHROME_ID_TEST"
        self.config_text = json.dumps(cfg, indent=1)  # JSON is YAML
        self.write("pipeline/config.yaml", self.config_text)
        for stage in cfg["stage_defs"].values():  # `check` wants every stage file it names
            if str(stage.get("run", "")).endswith(".md"):
                self.write(stage["run"], "# stage stub\n")

        self.expected_seen = {"seen": self.seen_fixture()}
        self.write("job_scraper/seen_jobs.json", rank_state_dump(self.expected_seen))
        self.write("job_search_tracker.csv", csv_text([[
            "2026-10-20", "Vandelay Industries", "Logistics", "Data Analyst", "full-time", "referral",
            "applied", "", "71", '=1+1, then +1 555 0100\nsecond line, "quoted"',
            "cv/main_vandelay_data_analyst.tex", "cover_letters/cover_vandelay_data_analyst.tex",
            "https://jobs.example.com/vandelay/9", ""]]))
        self.write_old_run()

    # ---- fixtures

    @staticmethod
    def ranked(title, company, number, score, verdict, **over) -> dict:
        entry = {"title": title, "company": company, "url": f"https://jobs.example.com/{number}",
                 "status": "ranked", "rank_score": score, "rank_verdict": verdict,
                 "location": "Springfield (hybrid)", "location_verdict": "PASS", "language_gate": "PASS",
                 "guard": "clean", "first_seen": "2026-11-06", "rank_date": "2026-11-08",
                 "posted_date": "2026-11-01", "deadline": None, "portal": "example-portal",
                 "strengths": ["evals", "agents"], "gaps": []}
        entry.update(over)
        return entry

    def seen_fixture(self) -> dict:
        return {
            "acme-1001": self.ranked("Machine Learning Engineer", "Acme Corp", 1001, 88, "Strong Fit",
                                     salary="$150,000 - $170,000"),
            "acme-1003": self.ranked("Data Scientist", "Acme Corp", 1003, 81, "Strong Fit"),
            "globex-2002": self.ranked("Applied AI Engineer", "Globex", 2002, 76, "Good Fit"),
            "initech-3001": {k: v for k, v in self.ranked("LLM Engineer", "Initech", 3001, 92, "Strong Fit").items()
                             if k != "guard"},
            "hooli-5001": self.ranked("AI Solutions Engineer", "Hooli", 5001, 55, "Possible Fit"),
            "umbrella-4001": {
                "title": '=HYPERLINK("https://example.com","Apply")', "company": "007",
                "location": "+1 555 0100", "status": "new", "rank_verdict": "TRUE", "salary": "3.50",
                "notes": "'x", "summary": "line one\nline two\tZürich, 東京 ", "remote": False,
                "rank_score": None, "deadline": "2026-12-01", "first_seen": "2026-11-07",
                "url": "https://jobs.example.com/4001", "extra": {"z": 1, "a": [True, None, 2.5]},
            },
        }

    def write_old_run(self) -> None:
        """An old pipeline/runs folder: Globex errored on 2026-11-08 (ledger-from-runs input)."""
        self.write("pipeline/runs/2026-11-08/run.json", json.dumps(
            {"run_id": "2026-11-08", "started_at": "2026-11-08T00:05:00-08:00"}))
        self.write("pipeline/runs/2026-11-08/manifest.json", json.dumps({
            "run_id": "2026-11-08", "created_at": "2026-11-08T00:15:00-08:00",
            "jobs": [{"key": "globex-2002", "title": "Applied AI Engineer", "company": "Globex",
                      "url": "https://jobs.example.com/2002", "rank_score": 76, "rank_verdict": "Good Fit",
                      "attempt": 1, "fired_at": "2026-11-08T00:20:00-08:00", "fire_count": 1}]}))
        self.write("pipeline/runs/2026-11-08/lanes/globex-2002.json", json.dumps({
            "status": "error", "reason": "the LaTeX compile failed", "key": "globex-2002",
            "pipeline_run": "2026-11-08", "phase": "documents", "updated_at": "2026-11-08T01:10:00-08:00"}))
        self.write("pipeline/runs/2026-11-08/report.md", "# Nightly pipeline: 2026-11-08\n")

    # ---- plumbing

    def write(self, rel: str, text: str, root=None) -> Path:
        path = (root or self.root) / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def cli(self, tool: Path, *args, root=None, now=None, stdin=None, expect=0):
        argv = [sys.executable, str(tool), "--root", str(root or self.root)]
        if now:
            argv += ["--now", now]
        proc = subprocess.run(argv + [str(a) for a in args], input=stdin, capture_output=True,
                              text=True, encoding="utf-8")
        self.assertEqual(proc.returncode, expect,
                         f"{tool.name} {' '.join(map(str, args))}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
        self.stderr = proc.stderr
        return json.loads(proc.stdout) if proc.stdout.strip() else None

    def sync(self, *args, root=None, expect=0):
        return self.cli(TOOL, *args, root=root, expect=expect)

    def state(self, now: str, *args, root=None, stdin=None, expect=0):
        return self.cli(STATE_TOOL, *args, root=root, now=now, stdin=stdin, expect=expect)

    def lane_write(self, now, key, phase, payload, root=None, expect=0):
        return self.state(now, "write-lane", "--run-id", NIGHT, "--key", key, "--phase", phase,
                          root=root, stdin=json.dumps(payload), expect=expect)

    def check_claim(self, now, key, phase, nonce, root=None, expect=0):
        return self.state(now, "check-claim", "--run-id", NIGHT, "--key", key, "--phase", phase,
                          "--nonce", nonce, root=root, expect=expect)

    def local_ledger(self, root=None) -> list:
        return json.loads(((root or self.root) / "pipeline" / "ledger.json").read_text(encoding="utf-8"))["rows"]

    def export(self, tab: str, *extra, root=None, expect=0):
        """drive-sync "Export a tab": every page in order, through drive_sync.py export."""
        size = self.LEDGER_PAGE if tab == "ledger" else 200
        args = ["export", tab]
        for page in self.sheet.save_pages(tab, self.scratch, size):
            args += ["--values", page]
        return self.sync(*args, *extra, root=root, expect=expect)

    def assert_nothing_to_write(self, tab: str, root=None) -> None:
        plan = self.sync("rows", tab, "--from", ds.DEFAULT_FILES[tab], root=root)
        self.assertEqual((plan["append"], plan["update"]), ([], []), f"{tab} still differs from the Sheet")

    def assert_seen_round_trip(self) -> None:
        out = self.scratch / "seen_round_trip.json"
        self.export("seen_jobs", "--out", out)
        self.assertEqual(json.dumps(json.loads(out.read_text(encoding="utf-8")), sort_keys=True),
                         json.dumps(self.expected_seen, sort_keys=True))

    def push_ledger(self, root=None, others=()) -> list:
        """drive-sync "Write a tab" for the ledger (no refresh before `rows`; export after), plus
        the integration checks. `others` are rows another session appended to the tab since this
        working copy's last export; they land between its numbered rows and its new ones."""
        before = self.local_ledger(root)
        problems = ds.validate_ledger([(f"local row {i}", row, None) for i, row in enumerate(before, 1)])
        self.assertEqual(problems, [], "drive_sync.py rejects ledger rows pipeline_state.py wrote")
        numbered = [r for r in before if r["row"] is not None]
        pending = [r for r in before if r["row"] is None]
        plan = self.sync("rows", "ledger", "--from", "pipeline/ledger.json", root=root)
        self.assertEqual(plan["counts"]["append"], len(pending))
        self.assertEqual(plan["update"], [])
        self.sheet.apply(plan)
        self.export("ledger", root=root)
        after = self.local_ledger(root)
        self.assertEqual(after[:len(numbered)], numbered)
        self.assertEqual([without_row(r) for r in after[len(numbered):]],
                         [without_row(r) for r in [*others, *pending]])
        self.assertEqual([r["row"] for r in after], list(range(2, len(after) + 2)))
        self.assert_nothing_to_write("ledger", root)
        self.assert_seen_round_trip()
        return after

    def push_tracker(self) -> dict:
        before = csv_body(self.root / "job_search_tracker.csv")
        plan = self.sync("rows", "tracker", "--from", "job_search_tracker.csv")
        self.sheet.apply(plan)
        self.export("tracker")
        self.assertEqual(csv_body(self.root / "job_search_tracker.csv"), before)
        self.assertEqual(self.sheet.tabs["tracker"], before)
        self.assert_nothing_to_write("tracker")
        self.assert_seen_round_trip()
        return plan

    def push_seen(self) -> dict:
        """drive-sync "Write a tab" for seen_jobs: a fresh export to scratch, rows, write, export
        to scratch again, and `rows` must then have nothing left to write."""
        fresh = self.scratch / "fresh_seen_jobs.json"
        if self.sheet.tabs["seen_jobs"]:
            self.export("seen_jobs", "--out", fresh)
        plan = self.sync("rows", "seen_jobs", "--from", "job_scraper/seen_jobs.json")
        self.sheet.apply(plan)
        self.export("seen_jobs", "--out", fresh)
        self.assert_nothing_to_write("seen_jobs")
        self.export("seen_jobs", "--verify-against", "job_scraper/seen_jobs.json")
        self.assertEqual(json.loads(fresh.read_text(encoding="utf-8")), self.expected_seen)
        self.assert_seen_round_trip()
        return plan

    def sheet_row(self, tab: str, key: str) -> int:
        return next(n for n, row in enumerate(self.sheet.tabs[tab], 1) if row[0] == key)

    # ---- the walk

    def test_a_whole_night_and_morning_through_both_tools(self):
        run_id = self.first_load_and_start()
        self.night(run_id)
        self.morning(run_id)
        self.assert_final_sheet()

    def first_load_and_start(self) -> str:
        # The applicant's first load. Every tab is empty, so exporting one is refused with the
        # first-load instruction, and nothing local is touched.
        for tab in ds.TABS:
            self.export(tab, expect=2)
            self.assertIn("no header row", self.stderr)
            self.assertIn(f"`rows {tab}` without a snapshot", self.stderr)
        self.assertFalse((self.root / "pipeline" / "ledger.json").exists())
        self.assertEqual(json.loads((self.root / "job_scraper/seen_jobs.json").read_text(encoding="utf-8")),
                         self.expected_seen)

        # Load: seen_jobs and the tracker as they are, the ledger from the old run folders.
        plan = self.push_seen()
        self.assertTrue(plan["header"])
        self.assertEqual(plan["append"][0]["values"][0], ["'" + h for h in ds.SEEN_HEADER])
        self.assertEqual(plan["counts"]["append"], len(self.expected_seen["seen"]))
        plan = self.push_tracker()
        self.assertTrue(plan["header"])
        self.assertEqual(plan["counts"], {"append": 1, "update": 0, "unchanged": 0, "missing_locally": 0})
        migrated = self.state(pacific("22:00", "2026-11-08"), "ledger-from-runs")
        self.assertEqual(migrated["migrated_runs"], ["2026-11-08"])
        self.assertEqual(migrated["warnings"], [])
        loaded = self.push_ledger()
        self.assertEqual([(r["event"], r["key"], r["status"]) for r in loaded], [
            ("started", "", ""), ("selected", "globex-2002", ""), ("fired", "globex-2002", ""),
            ("claimed", "globex-2002", "running"), ("finished", "globex-2002", "error"),
            ("run_finished", "", "done")])

        # Part 0 of the nightly run: export the three tabs into the working copy.
        self.export("seen_jobs")
        self.assertEqual(json.loads((self.root / "job_scraper/seen_jobs.json").read_text(encoding="utf-8")),
                         self.expected_seen)
        self.export("tracker")
        self.export("ledger")
        self.assertEqual(self.local_ledger(), loaded)

        # Part 1: check, new-run, push, check-run with the printed nonce.
        check = self.state(pacific("00:01"), "check")
        self.assertTrue(check["ok"], check["problems"])
        self.assertEqual(check["ledger"], {"rows": len(loaded), "unpushed": 0})
        run = self.state(pacific("00:02"), "new-run")
        self.assertEqual(run["run_id"], NIGHT)
        self.state(pacific("00:02"), "check-run", "--run-id", NIGHT, "--nonce", run["nonce"], expect=1)
        self.assertIn("not in the Sheet yet", self.stderr)
        self.push_ledger()
        owned = self.state(pacific("00:03"), "check-run", "--run-id", NIGHT, "--nonce", run["nonce"])
        self.assertTrue(owned["ok"])
        self.state(pacific("00:03"), "check-run", "--run-id", NIGHT, "--nonce", "0" * 32, expect=5)
        self.state(pacific("00:04"), "new-run", expect=5)  # another nightly start is refused
        self.run_nonce = run["nonce"]
        return run["run_id"]

    def night(self, run_id: str) -> None:
        # Part 2: scrape and rank changed the job database; push it (one append, one update).
        seen = self.expected_seen["seen"]
        seen["hooli-5001"].update(rank_score=58, rank_date="2026-11-09")
        seen["wayne-6001"] = {"title": "Research Engineer, Evals", "company": "Wayne Enterprises",
                              "url": "https://jobs.example.com/6001", "status": "new",
                              "first_seen": "2026-11-09", "deadline": None, "portal": "example-portal"}
        self.write("job_scraper/seen_jobs.json", rank_state_dump(self.expected_seen))
        hooli_row = self.sheet_row("seen_jobs", "hooli-5001")
        plan = self.push_seen()
        self.assertEqual(plan["counts"], {"append": 1, "update": 1, "unchanged": 5, "missing_locally": 0})
        self.assertEqual([u["row"] for u in plan["update"]], [hooli_row])

        # Part 3: select, push, lanes, mark-fired, push.
        sel = self.state(pacific("00:20"), "select", "--run-id", run_id)
        self.assertEqual([(j["key"], j["attempt"]) for j in sel["selected"]],
                         [("acme-1001", 1), ("globex-2002", 2)])
        reasons = {s["key"]: s["reason"] for s in sel["skipped"]}
        self.assertIn("max_per_company", reasons["acme-1003"])
        self.assertIn("integrity verdict", reasons["initech-3001"])
        self.assertTrue(all(r["detail"]["browser"] for r in sel["ledger_rows"]))
        self.push_ledger()
        lanes = self.state(pacific("00:21"), "lanes", "--run-id", run_id)
        self.assertEqual([m["key"] for m in lanes["start_documents"]], ["acme-1001", "globex-2002"])
        self.assertEqual(lanes["start_browser"], [])
        for message in lanes["start_documents"]:
            self.state(pacific("00:21"), "mark-fired", "--run-id", message["pipeline_run"],
                       "--key", message["key"], "--phase", message["phase"])
        self.push_ledger()

        # The Acme documents lane, plus a duplicate session for the same job that exported the
        # ledger before either claim reached the Sheet: both claims get pushed, the first wins.
        twin = self.base / "twin"
        self.write("pipeline/config.yaml", self.config_text, root=twin)
        self.export("ledger", root=twin)
        self.export("ledger")
        claim = self.lane_write(pacific("00:25"), "acme-1001", "documents", {"status": "running"})
        twin_claim = self.lane_write(pacific("00:25"), "acme-1001", "documents", {"status": "running"}, root=twin)
        self.assertNotEqual(claim["nonce"], twin_claim["nonce"])
        self.push_ledger()
        self.check_claim(pacific("00:26"), "acme-1001", "documents", claim["nonce"])
        self.push_ledger(root=twin, others=claim["ledger_rows"])
        out = self.check_claim(pacific("00:26"), "acme-1001", "documents", twin_claim["nonce"], root=twin, expect=3)
        self.assertEqual((out["ok"], out["claims"]), (False, 2))
        self.export("ledger")
        out = self.check_claim(pacific("00:27"), "acme-1001", "documents", claim["nonce"])
        self.assertEqual((out["ok"], out["claims"]), (True, 2))
        self.lane_write(pacific("00:27"), "acme-1001", "documents", {"status": "running"}, expect=3)

        # The Globex documents lane (its second attempt) stops for review.
        self.export("ledger")
        g_claim = self.lane_write(pacific("00:30"), "globex-2002", "documents", {"status": "running"})
        self.push_ledger()
        self.check_claim(pacific("00:31"), "globex-2002", "documents", g_claim["nonce"])
        self.lane_write(pacific("01:10"), "globex-2002", "documents", {
            "status": "needs_review", "stage_reached": "apply", "company": "Globex",
            "role": "Applied AI Engineer", "reason": "files for this company and role already exist in Drive"})
        self.push_ledger()

        # Acme drafts, with a tracker_row, then pushes its files.
        name = "acme_corp_machine_learning_engineer"
        self.files = {"cv": f"cv/main_{name}.tex", "cover_letter": f"cover_letters/cover_{name}.tex",
                      "job_posting": f"documents/applications/{name}/job_posting.md",
                      "answers": f"documents/applications/{name}/application_form_answers.md",
                      "cv_pdf": f"cv/main_{name}.pdf"}
        for rel in self.files.values():
            self.write(rel, f"% {rel}\n")
        self.write(f"pipeline/runs/{run_id}/lanes/acme-1001.md", "## Documents\nDrafted.\n")
        self.trow = {"date": NIGHT, "company": "Acme Corp", "sector": "Software",
                     "role": "Machine Learning Engineer", "role_type": "ML engineering",
                     "channel": "company portal", "status": "drafted", "contact_person": "", "fit_rating": 84,
                     "notes": "Job ID =A-1001; pay +150k; \"hybrid\"\nnightly pipeline draft, not submitted",
                     "cv_file": self.files["cv"], "cover_letter_file": self.files["cover_letter"],
                     "source": "https://jobs.example.com/1001", "deadline": ""}
        self.lane_write(pacific("01:30"), "acme-1001", "documents", {
            "status": "drafted", "stage_reached": "form-answers", "company": "Acme Corp",
            "role": "Machine Learning Engineer", "folder": f"documents/applications/{name}",
            "apply_score": 84, "apply_verdict": "Strong Fit",
            "application_page": "https://jobs.example.com/1001/apply", "files": self.files,
            "left_for_you": ["salary expectation"], "please_check": ["start date"], "questions": [],
            "notes": "line one\nline two", "tracker_row": self.trow})
        self.push_ledger()
        plan = self.sync("push-plan", "--role", "documents-lane", "--run-id", run_id, "--key", "acme-1001",
                         "--job-folder", name, "--company", "Acme Corp")
        self.assertEqual(plan["refused"], [])
        self.assertEqual({(f["path"], f["action"]) for f in plan["files"]},
                         {(p, "create") for p in [self.files["cv"], self.files["cover_letter"],
                                                  self.files["job_posting"], self.files["answers"],
                                                  f"pipeline/runs/{run_id}/lanes/acme-1001.json",
                                                  f"pipeline/runs/{run_id}/lanes/acme-1001.md"]})

        # Part 4: the fan-in sees both lanes finished.
        self.export("ledger")
        lanes = self.state(pacific("01:40"), "lanes", "--run-id", run_id)
        self.assertEqual((lanes["waiting"], lanes["start_documents"]), (0, []))
        self.assertEqual({j["key"]: (j["documents"], j["browser"]) for j in lanes["jobs"]},
                         {"acme-1001": ("drafted", "ready"), "globex-2002": ("needs_review", "n/a")})

        # Part 5: record (fresh tracker export, record, push tracker and ledger), report, finish.
        self.export("tracker")
        rec = self.state(pacific("01:45"), "record")
        self.assertEqual([(a["key"], a["action"]) for a in rec["actions"]], [("acme-1001", "appended")])
        expected_row = [str(self.trow[h]) for h in ds.TRACKER_HEADER]
        self.assertEqual(csv_body(self.root / "job_search_tracker.csv")[-1], expected_row)
        plan = self.push_tracker()
        self.assertEqual(plan["counts"], {"append": 1, "update": 0, "unchanged": 1, "missing_locally": 0})
        self.assertEqual(self.sheet.tabs["tracker"][-1], expected_row)
        self.push_ledger()
        again = self.state(pacific("01:46"), "record")
        self.assertEqual((again["added"], again["ledger_rows"], again["already_recorded"]), ([], [], 1))

        self.state(pacific("01:47"), "stats")
        self.write(f"pipeline/runs/{run_id}/report.md", f"# Nightly pipeline: {run_id}\n")
        plan = self.sync("push-plan", "--role", "orchestrator", "--run-id", run_id)
        self.assertEqual(plan["refused"], [])
        self.assertEqual({f["path"] for f in plan["files"]},
                         {f"pipeline/runs/{run_id}/{p}" for p in (
                             "run.json", "manifest.json", "report.md", "lanes/acme-1001.json",
                             "lanes/acme-1001.md", "lanes/globex-2002.json")})
        done = self.state(pacific("01:50"), "finish-run", "--run-id", run_id, "--nonce", self.run_nonce)
        self.assertEqual(done["status"], "done")
        self.push_ledger()

    def morning(self, run_id: str) -> None:
        # The morning filler: export, check, fill-start, push, check-run.
        self.export("ledger")
        self.assertTrue(self.state(pacific("07:30"), "check")["ok"])
        fill = self.state(pacific("07:30"), "fill-start")
        fill_id = fill["fill_id"]
        self.assertEqual(fill_id, f"fill-{NIGHT}")
        self.push_ledger()
        self.assertTrue(self.state(pacific("07:31"), "check-run", "--run-id", fill_id, "--nonce", fill["nonce"])["ok"])

        # fill-queue offers the drafted Acme job under its nightly run id; fire it.
        queue = self.state(pacific("07:31"), "fill-queue")
        message = {"pipeline_run": run_id, "phase": "browser", "key": "acme-1001",
                   "url": "https://jobs.example.com/1001", "title": "Machine Learning Engineer",
                   "company": "Acme Corp", "rank_score": 88, "rank_verdict": "Strong Fit", "attempt": 1}
        self.assertEqual((queue["ready"], queue["start_browser"], queue["held"], queue["running"]),
                         ([message], [message], [], 0))
        # A real filler session holds only what `paths --role filler` lists (the config, the
        # ledger tab and the tracker tab, which fill-queue checks for jobs already applied for);
        # fill-queue there offers the same job, with the tracker checked.
        filler = self.base / "filler"
        self.write("pipeline/config.yaml", self.config_text, root=filler)
        listed = self.sync("paths", "--role", "filler", "--fill-id", fill_id, root=filler)
        self.assertEqual(listed["pull"]["tabs"], ["ledger", "tracker"])
        for tab in listed["pull"]["tabs"]:
            self.export(tab, root=filler)
        self.assertEqual(csv_body(filler / "job_search_tracker.csv"), csv_body(self.root / "job_search_tracker.csv"))
        filler_queue = self.state(pacific("07:31"), "fill-queue", root=filler)
        self.assertEqual((filler_queue["start_browser"], filler_queue["tracker_checked"]), ([message], True))
        self.assertNotIn("warning", filler_queue)
        self.state(pacific("07:31"), "mark-fired", "--run-id", message["pipeline_run"], "--key", message["key"],
                   "--phase", message["phase"])
        self.push_ledger()

        # The browser lane: it pulls the documents result and every file it names (not the PDF),
        # claims, pushes, checks its claim, fills, and pushes its result.
        paths = self.sync("paths", "--role", "browser-lane", "--run-id", run_id, "--key", "acme-1001")
        pulls = paths["pull"]["files"]
        self.assertIn(f"pipeline/runs/{run_id}/lanes/acme-1001.json", pulls)
        for name in ("cv", "cover_letter", "answers", "job_posting"):
            self.assertIn(self.files[name], pulls)
        self.assertNotIn(self.files["cv_pdf"], pulls)
        self.export("ledger")
        b_claim = self.lane_write(pacific("07:33"), "acme-1001", "browser", {"status": "running"})
        self.push_ledger()
        self.check_claim(pacific("07:34"), "acme-1001", "browser", b_claim["nonce"])
        busy = self.state(pacific("07:40"), "fill-queue")
        self.assertEqual((busy["ready"], busy["start_browser"], busy["running"]), ([], [], 1))
        self.lane_write(pacific("07:55"), "acme-1001", "browser", {
            "status": "filled", "application_page": "https://jobs.example.com/1001/apply", "tab_id": 4242,
            "filled": ["name", "email", "CV upload", "cover letter upload"],
            "left_for_you": ["salary expectation"], "please_check": [], "questions": [],
            "notes": "stopped before Submit"})
        self.write(f"pipeline/runs/{run_id}/lanes/acme-1001.browser.md", "## Browser\nFilled.\n")
        self.push_ledger()
        plan = self.sync("push-plan", "--role", "browser-lane", "--run-id", run_id, "--key", "acme-1001")
        self.assertEqual(({f["path"] for f in plan["files"]}, plan["refused"]),
                         ({f"pipeline/runs/{run_id}/lanes/acme-1001.browser.json",
                           f"pipeline/runs/{run_id}/lanes/acme-1001.browser.md"}, []))

        # The filler's fan-in finds the queue empty, reports, finishes; stats sees the whole story.
        self.export("ledger")
        empty = self.state(pacific("08:00"), "fill-queue")
        self.assertEqual((empty["ready"], empty["running"], empty["held"]), ([], 0, []))
        self.write(f"pipeline/fills/{fill_id}/report.md", f"# Morning filling: {fill_id}\n")
        plan = self.sync("push-plan", "--role", "filler", "--fill-id", fill_id)
        self.assertEqual([f["path"] for f in plan["files"]], [f"pipeline/fills/{fill_id}/report.md"])
        done = self.state(pacific("08:01"), "fill-finish", "--fill-id", fill_id, "--nonce", fill["nonce"])
        self.assertEqual(done["status"], "done")
        self.push_ledger()
        stats = self.state(pacific("08:02"), "stats")
        self.assertEqual(stats["lane_outcomes_all_runs"], {"error": 1, "filled": 1, "needs_review": 1})
        self.assertEqual(stats["drafts_too_recent_to_judge"], 1)

    def assert_final_sheet(self) -> None:
        ledger = self.local_ledger()
        self.assertEqual(self.sheet.tabs["ledger"][0], ds.LEDGER_HEADER)
        self.assertEqual(self.sheet.tabs["ledger"][1:], [
            [r["at"], r["run_id"], r["key"], r["phase"], r["event"], r["status"],
             json.dumps(r["detail"], sort_keys=True, ensure_ascii=False, separators=(",", ":"))]
            for r in ledger])
        self.assertEqual([(r["run_id"], r["key"], r["phase"], r["event"]) for r in ledger
                          if r["run_id"] != "2026-11-08"], [
            (NIGHT, "", "run", "started"),
            (NIGHT, "acme-1001", "documents", "selected"), (NIGHT, "globex-2002", "documents", "selected"),
            (NIGHT, "acme-1001", "documents", "fired"), (NIGHT, "globex-2002", "documents", "fired"),
            (NIGHT, "acme-1001", "documents", "claimed"), (NIGHT, "acme-1001", "documents", "claimed"),
            (NIGHT, "globex-2002", "documents", "claimed"), (NIGHT, "globex-2002", "documents", "finished"),
            (NIGHT, "acme-1001", "documents", "finished"), (NIGHT, "acme-1001", "documents", "recorded"),
            (NIGHT, "", "run", "run_finished"),
            (f"fill-{NIGHT}", "", "run", "started"), (NIGHT, "acme-1001", "browser", "fired"),
            (NIGHT, "acme-1001", "browser", "claimed"), (NIGHT, "acme-1001", "browser", "finished"),
            (f"fill-{NIGHT}", "", "run", "run_finished")])
        self.assertEqual(self.sheet.tabs["tracker"], csv_body(self.root / "job_search_tracker.csv"))
        self.assertEqual(len(self.sheet.tabs["tracker"]), 3)
        self.assertEqual([row[0] for row in self.sheet.tabs["seen_jobs"]],
                         ["key", "acme-1001", "acme-1003", "globex-2002", "initech-3001", "hooli-5001",
                          "umbrella-4001", "wayne-6001"])
        self.assert_seen_round_trip()
        for tab in ds.TABS:
            self.assert_nothing_to_write(tab)


if __name__ == "__main__":
    unittest.main()
