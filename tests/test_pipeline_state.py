"""Tests for pipeline/pipeline_state.py on the ledger (DESIGN-core.md).

The nightly pipeline keeps its history in the Sheet's ledger tab, exported to
pipeline/ledger.json. These pin that every selection rule, cap, retry rule and
duplicate-start protection works from ledger rows alone: the per-company cap,
retries after an error or an unfinished lane, "still running", duplicate
claims, one browser lane at a time, late drafts recorded, held-back integrity
verdicts, whole-word company exclusion, the new-run / check-run guard, the
test-run cap, fill runs, and the one-time ledger-from-runs migration.

Every test builds a throwaway working copy (pipeline/config.yaml,
job_scraper/seen_jobs.json, job_search_tracker.csv, pipeline/ledger.json) and
drives the tool through subprocess with --root and a fixed --now.
"""
import csv
import json
import shutil
import subprocess
import sys
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

REPO = Path(__file__).resolve().parent.parent
TOOL = REPO / "pipeline" / "pipeline_state.py"

NOW = "2026-11-09T01:00:00-08:00"
RUN = "2026-11-09"
TRACKER_HEADER = [
    "date", "company", "sector", "role", "role_type", "channel", "status",
    "contact_person", "fit_rating", "notes", "cv_file", "cover_letter_file",
    "source", "deadline",
]


def base_config() -> dict:
    return {
        "version": 1,
        "timezone": "America/Los_Angeles",
        "drive": {
            "root_id": "FOLDER_ID_ROOT",
            "sheet_id": "SHEET_ID_TEST",
            "folders": {},
            "overlay": ["CLAUDE.md", "cv/main_example.tex"],
            "replaceable": ["job_scraper/notion_sync.json", "company_research/*.json"],
        },
        "selection": {
            "max_applications": 3, "min_rank_score": 60, "verdicts": [], "max_per_company": 1,
            "skip_location_flag": True, "skip_language_flag": True, "skip_guard_flagged": True,
            "max_rank_age_days": 21, "max_posting_age_days": 45, "exclude_companies": [],
            "include_title_keywords": [], "exclude_title_keywords": [],
            "retry_errored_lanes": True, "max_lane_attempts": 2,
        },
        "stages": {
            "before_lanes": ["scrape", "rank"],
            "lane": ["apply", "form-answers", "autofill"],
            "after_lanes": ["record", "report"],
        },
        "stage_defs": {
            "scrape": {"run": ".claude/skills/job-scraper/SKILL.md"},
            "rank": {"run": ".claude/commands/rank.md"},
            "apply": {"run": ".claude/commands/apply.md"},
            "form-answers": {"run": "pipeline/skills/form-answers/SKILL.md"},
            "autofill": {"run": "anthropic-skills:job-application-autofill", "phase": "browser",
                         "browser": "chrome", "chrome_device_id": "CHROME_ID_TEST", "enabled": True},
            "record": {"run": "builtin"},
            "report": {"run": "builtin"},
        },
        "lanes": {
            "documents_task_id": "TASK_ID_DOCUMENTS", "browser_task_id": "TASK_ID_BROWSER",
            "max_parallel": 3, "max_parallel_browser": 1, "first_check_minutes": 60,
            "check_every_minutes": 20, "browser_check_minutes": 10, "max_wait_minutes": 360,
            "max_fill_age_days": 7,
        },
    }


def entry(**over) -> dict:
    base = {
        "title": "Machine Learning Engineer", "company": "Acme", "url": "https://jobs.example.com/acme/1",
        "status": "ranked", "rank_score": 80, "rank_verdict": "Strong Fit",
        "location_verdict": "PASS", "language_gate": "PASS", "guard": "clean",
        "first_seen": "2026-11-02", "rank_date": "2026-11-05", "posted_date": "2026-11-01",
        "deadline": None, "portal": "example-portal",
    }
    base.update(over)
    return base


def job_detail(key, company="Acme", title="Machine Learning Engineer", browser=True, attempt=1) -> dict:
    return {"url": f"https://jobs.example.com/{key}", "title": title, "company": company,
            "rank_score": 80, "rank_verdict": "Strong Fit", "attempt": attempt, "browser": browser}


def tracker_row(company, role, source, **over) -> dict:
    slug = company.lower().replace(" ", "-")
    row = {"date": "2026-11-07", "company": company, "sector": "Software", "role": role,
           "role_type": "ML engineering", "channel": "portal", "status": "drafted",
           "contact_person": "", "fit_rating": "78", "notes": "pipeline draft",
           "cv_file": f"cv/main_{slug}.tex", "cover_letter_file": f"cover_letters/cover_{slug}.tex",
           "source": source, "deadline": ""}
    row.update(over)
    return row


class LedgerCase(unittest.TestCase):
    """A temp working copy plus helpers to write ledger rows and run the tool."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        (self.root / "pipeline").mkdir()
        (self.root / "job_scraper").mkdir()
        self.config = base_config()
        self.seen: dict = {}
        self.rows: list = []
        self.next_row = 2  # row 1 of the Sheet is the header
        self.tracker_text = ",".join(TRACKER_HEADER) + "\n"

    # ---- fixtures

    def add(self, at, run_id, event, key="", phase=None, status="", detail=None, pushed=True):
        if phase is None:
            phase = "run" if event in ("started", "run_finished") else "documents"
        number = None
        if pushed:
            number = self.next_row
            self.next_row += 1
        self.rows.append({"row": number, "at": at, "run_id": run_id, "key": key, "phase": phase,
                          "event": event, "status": status, "detail": detail if detail is not None else {}})

    def start(self, run_id, at, **kw):
        self.add(at, run_id, "started", detail={"test": False, "max_applications": None}, **kw)

    def end(self, run_id, at, status="done"):
        self.add(at, run_id, "run_finished", status=status, detail={"reason": None})

    def pick(self, run_id, key, at, **detail):
        self.add(at, run_id, "selected", key=key, detail=job_detail(key, **detail))

    def lane(self, run_id, key, event, at, phase="documents", status="", detail=None, pushed=True):
        self.add(at, run_id, event, key=key, phase=phase, status=status, detail=detail, pushed=pushed)

    def finished(self, run_id, key, at, status="drafted", phase="documents", trow=None, **result):
        detail = {"status": status, "key": key, "pipeline_run": run_id, "phase": phase,
                  "updated_at": at, **result}
        if trow is not None:
            detail["tracker_row"] = trow
        self.lane(run_id, key, "finished", at, phase=phase, status=status, detail=detail)

    def recorded(self, run_id, key, at, company, role):
        self.lane(run_id, key, "recorded", at, detail={"company": company, "role": role})

    def save(self):
        (self.root / "pipeline" / "config.yaml").write_text(json.dumps(self.config, indent=1), encoding="utf-8")
        (self.root / "job_scraper" / "seen_jobs.json").write_text(
            json.dumps({"seen": self.seen}, indent=2), encoding="utf-8")
        (self.root / "pipeline" / "ledger.json").write_text(json.dumps({"rows": self.rows}, indent=1),
                                                             encoding="utf-8")
        (self.root / "job_search_tracker.csv").write_text(self.tracker_text, encoding="utf-8")

    def save_config(self):
        (self.root / "pipeline" / "config.yaml").write_text(json.dumps(self.config, indent=1), encoding="utf-8")

    def ledger(self) -> list:
        return json.loads((self.root / "pipeline" / "ledger.json").read_text(encoding="utf-8"))["rows"]

    def write_ledger(self, rows):
        (self.root / "pipeline" / "ledger.json").write_text(json.dumps({"rows": rows}, indent=1),
                                                             encoding="utf-8")

    def push(self):
        """What a push plus re-export does: unpushed rows get the next Sheet row numbers."""
        rows = self.ledger()
        number = max([r["row"] for r in rows if r["row"] is not None] or [1]) + 1
        for row in rows:
            if row["row"] is None:
                row["row"] = number
                number += 1
        self.write_ledger(rows)

    def run_tool(self, *args, expect=0, stdin=None, now=NOW):
        proc = subprocess.run(
            [sys.executable, str(TOOL), "--root", str(self.root), "--now", now, *args],
            input=stdin, capture_output=True, text=True, encoding="utf-8",
        )
        self.assertEqual(proc.returncode, expect, f"{args}\nstdout={proc.stdout}\nstderr={proc.stderr}")
        self.last_stdout = proc.stdout
        text = proc.stdout if proc.stdout.strip() else proc.stderr
        return json.loads(text) if text.strip() else {}

    @staticmethod
    def reasons(out) -> dict:
        return {s["key"]: s["reason"] for s in out["skipped"]}


# ---------------------------------------------------------------- select

class SelectFromLedgerTest(LedgerCase):
    def test_per_company_cap(self):
        self.seen = {
            "acme_ml-engineer": entry(company="Acme", rank_score=90, url="https://jobs.example.com/1"),
            "acme-inc_data-scientist": entry(company="Acme Inc", title="Data Scientist", rank_score=85,
                                             url="https://jobs.example.com/2"),
            "globex_ml-engineer": entry(company="Globex", rank_score=70, url="https://jobs.example.com/3"),
        }
        self.start(RUN, "2026-11-09T00:05:00-08:00")
        self.save()
        out = self.run_tool("select", "--run-id", RUN)
        self.assertEqual([j["key"] for j in out["selected"]], ["acme_ml-engineer", "globex_ml-engineer"])
        self.assertEqual(self.reasons(out)["acme-inc_data-scientist"],
                         "max_per_company reached tonight; eligible next run")
        picks = [r for r in self.ledger() if r["event"] == "selected"]
        self.assertEqual([r["key"] for r in picks], ["acme_ml-engineer", "globex_ml-engineer"])
        self.assertEqual(out["ledger_rows"], picks)
        for row in picks:
            self.assertIsNone(row["row"])
            self.assertEqual((row["run_id"], row["phase"], row["status"]), (RUN, "documents", ""))
            self.assertEqual(set(row["detail"]), {"url", "title", "company", "rank_score", "rank_verdict",
                                                  "attempt", "browser"})
            self.assertIs(row["detail"]["browser"], True)
        manifest = json.loads((self.root / "pipeline" / "runs" / RUN / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual([j["key"] for j in manifest["jobs"]], ["acme_ml-engineer", "globex_ml-engineer"])

        again = self.run_tool("select", "--run-id", RUN)
        self.assertEqual(again["note"], "already selected for this run")
        self.assertEqual([j["key"] for j in again["selected"]], ["acme_ml-engineer", "globex_ml-engineer"])
        self.assertEqual(len(self.ledger()), 3, "a repeated select appends nothing")
        self.run_tool("select", "--run-id", RUN, "--force", expect=1)

    def test_retry_after_an_error_or_an_unfinished_lane(self):
        keys = {"acme_errored": ("Acme", 95), "globex_unfinished": ("Globex", 90),
                "initech_needs-review": ("Initech", 85), "umbrella_two-tries": ("Umbrella", 80),
                "hooli_never-started": ("Hooli", 75)}
        self.seen = {k: entry(company=c, rank_score=s, url=f"https://jobs.example.com/{k}")
                     for k, (c, s) in keys.items()}
        self.config["selection"]["max_applications"] = 5
        first = "2026-11-01"
        self.start(first, "2026-11-01T00:05:00-08:00")
        for key, (company, _score) in keys.items():
            self.pick(first, key, "2026-11-01T00:20:00-08:00", company=company)
        for key in ("acme_errored", "globex_unfinished", "initech_needs-review", "umbrella_two-tries"):
            self.lane(first, key, "fired", "2026-11-01T00:25:00-08:00")
        self.finished(first, "acme_errored", "2026-11-01T00:50:00-08:00", status="error", reason="form broke")
        self.finished(first, "initech_needs-review", "2026-11-01T01:00:00-08:00", status="needs_review")
        self.finished(first, "umbrella_two-tries", "2026-11-01T00:55:00-08:00", status="error")
        self.end(first, "2026-11-01T07:00:00-08:00")
        second = "2026-11-02"
        self.start(second, "2026-11-02T00:05:00-08:00")
        self.pick(second, "umbrella_two-tries", "2026-11-02T00:20:00-08:00", company="Umbrella", attempt=2)
        self.lane(second, "umbrella_two-tries", "fired", "2026-11-02T00:25:00-08:00")
        self.finished(second, "umbrella_two-tries", "2026-11-02T00:50:00-08:00", status="error")
        self.end(second, "2026-11-02T07:00:00-08:00")
        self.start(RUN, "2026-11-09T00:50:00-08:00")

        self.config["selection"]["retry_errored_lanes"] = False
        self.save()
        out = self.run_tool("select", "--run-id", RUN, "--dry-run")
        self.assertEqual([j["key"] for j in out["selected"]], ["hooli_never-started"])
        self.assertEqual(self.reasons(out), {
            "acme_errored": "already handled on run 2026-11-01 (error)",
            "globex_unfinished": "already handled on run 2026-11-01 (unfinished)",
            "initech_needs-review": "already handled on run 2026-11-01 (needs_review)",
            "umbrella_two-tries": "already handled on run 2026-11-02 (error)",
        })

        self.config["selection"]["retry_errored_lanes"] = True
        self.save_config()
        out = self.run_tool("select", "--run-id", RUN)
        self.assertEqual([(j["key"], j["attempt"]) for j in out["selected"]],
                         [("acme_errored", 2), ("globex_unfinished", 2), ("hooli_never-started", 1)])
        self.assertEqual(self.reasons(out), {
            "initech_needs-review": "already handled on run 2026-11-01 (needs_review)",
            "umbrella_two-tries": "already handled on run 2026-11-02 (error)",
        })
        self.assertEqual([r["detail"]["attempt"] for r in out["ledger_rows"]], [2, 2, 1])

    def test_job_started_on_an_earlier_run_and_still_running_is_not_picked(self):
        self.seen = {k: entry(company=c, url=f"https://jobs.example.com/{k}") for k, c in (
            ("acme_fired", "Acme"), ("umbrella_claimed", "Umbrella"),
            ("globex_picked", "Globex"), ("initech_stale", "Initech"))}
        self.start("2026-11-08", "2026-11-08T00:05:00-08:00")
        self.pick("2026-11-08", "initech_stale", "2026-11-08T00:20:00-08:00", company="Initech")
        self.lane("2026-11-08", "initech_stale", "fired", "2026-11-08T00:25:00-08:00")  # 24.5 h ago
        self.end("2026-11-08", "2026-11-08T07:00:00-08:00")
        earlier = "2026-11-08-2"
        self.start(earlier, "2026-11-08T23:00:00-08:00")
        for key, company in (("acme_fired", "Acme"), ("umbrella_claimed", "Umbrella"), ("globex_picked", "Globex")):
            self.pick(earlier, key, "2026-11-08T23:05:00-08:00", company=company)
        self.lane(earlier, "acme_fired", "fired", "2026-11-08T23:30:00-08:00")  # 90 minutes ago
        self.lane(earlier, "umbrella_claimed", "claimed", "2026-11-09T00:30:00-08:00", status="running",
                  detail={"nonce": "ab" * 16})  # a lane started by hand, never marked fired
        self.end(earlier, "2026-11-09T00:40:00-08:00")
        self.start(RUN, "2026-11-09T00:50:00-08:00")
        self.save()
        out = self.run_tool("select", "--run-id", RUN)
        self.assertEqual(self.reasons(out), {
            "acme_fired": "still running in run 2026-11-08-2",
            "umbrella_claimed": "still running in run 2026-11-08-2",
        })
        self.assertEqual(sorted((j["key"], j["attempt"]) for j in out["selected"]),
                         [("globex_picked", 1), ("initech_stale", 2)])

    def test_missing_integrity_verdict_is_held_back(self):
        self.seen = {
            "acme_no-verdict": entry(company="Acme", url="https://jobs.example.com/a"),
            "globex_flagged": entry(company="Globex", guard="flagged", url="https://jobs.example.com/b"),
            "initech_clean": entry(company="Initech", url="https://jobs.example.com/c"),
        }
        del self.seen["acme_no-verdict"]["guard"]
        self.save()
        out = self.run_tool("select", "--dry-run")
        self.assertEqual([j["key"] for j in out["selected"]], ["initech_clean"])
        self.assertEqual(self.reasons(out), {
            "acme_no-verdict": "no posting integrity verdict recorded: re-rank it or read the posting first",
            "globex_flagged": "posting integrity flag: read the posting first",
        })
        self.config["selection"]["skip_guard_flagged"] = False
        self.save_config()
        out = self.run_tool("select", "--dry-run")
        self.assertEqual(len(out["selected"]), 3)

    def test_company_exclusion_matches_whole_words(self):
        self.config["selection"].update({"exclude_companies": ["K&R", "Initech"], "max_applications": 5})
        self.seen = {
            "kr-services_ml": entry(company="K&R Services, Inc.", url="https://jobs.example.com/1"),
            "kr-labs_ml": entry(company="Kr Labs", url="https://jobs.example.com/2"),
            "mark-rogers_ml": entry(company="Mark&Rogers", url="https://jobs.example.com/3"),
            "initech-llc_ml": entry(company="Initech LLC", url="https://jobs.example.com/4"),
        }
        self.save()
        out = self.run_tool("select", "--dry-run")
        self.assertEqual(self.reasons(out), {"kr-services_ml": "company excluded in config",
                                             "initech-llc_ml": "company excluded in config"})
        self.assertEqual(sorted(j["key"] for j in out["selected"]), ["kr-labs_ml", "mark-rogers_ml"])

    def test_test_run_caps_the_pick(self):
        self.seen = {k: entry(company=c, rank_score=s, url=f"https://jobs.example.com/{k}")
                     for k, c, s in (("acme_ml", "Acme", 90), ("globex_ml", "Globex", 80),
                                     ("initech_ml", "Initech", 70))}
        self.save()
        new = self.run_tool("new-run", "--test", "--max-applications", "1")
        self.assertEqual(new["run_id"], RUN)
        detail = new["ledger_rows"][0]["detail"]
        self.assertEqual({k: detail[k] for k in ("test", "max_applications")}, {"test": True, "max_applications": 1})
        self.assertRegex(detail["nonce"], r"^[0-9a-f]{32}$")
        out = self.run_tool("select", "--run-id", RUN)
        self.assertEqual(out["max_applications"], 1)
        self.assertEqual([j["key"] for j in out["selected"]], ["acme_ml"])
        self.assertEqual(self.reasons(out), {
            "globex_ml": "over tonight's max_applications; eligible next run",
            "initech_ml": "over tonight's max_applications; eligible next run",
        })
        self.assertEqual(len([r for r in self.ledger() if r["event"] == "selected"]), 1)

    def test_no_browser_turns_the_browser_flag_off(self):
        self.seen = {"acme_ml": entry()}
        self.start(RUN, "2026-11-09T00:05:00-08:00")
        self.save()
        out = self.run_tool("select", "--run-id", RUN, "--no-browser", "Chrome is being reinstalled")
        self.assertIs(out["browser_enabled"], False)
        self.assertIs(out["ledger_rows"][0]["detail"]["browser"], False)
        manifest = json.loads((self.root / "pipeline" / "runs" / RUN / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["browser_off_reason"], "Chrome is being reinstalled")

    def test_select_needs_a_started_run_and_an_exported_ledger(self):
        self.seen = {"acme_ml": entry()}
        self.save()
        self.run_tool("select", "--run-id", RUN, expect=1)
        (self.root / "pipeline" / "ledger.json").unlink()
        err = self.run_tool("select", "--dry-run", expect=1)
        self.assertIn("ledger.json not found", err["error"])


# ---------------------------------------------------------------- runs

class RunGuardTest(LedgerCase):
    def test_new_run_refuses_while_a_run_is_active(self):
        self.start(RUN, "2026-11-09T00:00:00-08:00")  # an hour ago, no run_finished
        self.save()
        err = self.run_tool("new-run", expect=5)
        self.assertEqual(err["active_runs"], [RUN])
        self.end(RUN, "2026-11-09T00:30:00-08:00")
        self.write_ledger(self.rows)
        out = self.run_tool("new-run")
        self.assertEqual(out["run_id"], "2026-11-09-2", "run ids stay unique across the ledger")
        last = self.ledger()[-1]
        self.assertEqual((last["row"], last["run_id"], last["phase"], last["event"]),
                         (None, "2026-11-09-2", "run", "started"))
        self.assertEqual(last["detail"], {"test": False, "max_applications": None, "nonce": out["nonce"]})
        self.assertTrue((self.root / "pipeline" / "runs" / "2026-11-09-2" / "run.json").is_file())
        self.run_tool("new-run", expect=5)

    def test_a_stale_unfinished_run_does_not_block(self):
        self.start("2026-11-08", "2026-11-08T18:00:00-08:00")  # 7 hours ago, over max_wait_minutes
        self.save()
        self.assertEqual(self.run_tool("new-run")["run_id"], RUN)

    def test_check_run_lets_only_the_earliest_active_run_go_on(self):
        self.start("2026-11-09-3", "2026-11-09T00:42:00-08:00", pushed=False)  # not pushed, first in file
        self.start(RUN, "2026-11-09T00:40:00-08:00")
        self.start("2026-11-09-2", "2026-11-09T00:41:00-08:00")
        self.save()
        out = self.run_tool("check-run", "--run-id", RUN)
        self.assertTrue(out["ok"])
        out = self.run_tool("check-run", "--run-id", "2026-11-09-2", expect=5)
        self.assertEqual(out["earliest_active"], RUN)
        self.run_tool("check-run", "--run-id", "2026-11-09-3", expect=5)
        self.run_tool("check-run", "--run-id", "2026-11-01", expect=1)
        # The duplicate aborts itself; the earliest run is unaffected.
        done = self.run_tool("finish-run", "--run-id", "2026-11-09-2", "--aborted", "duplicate start")
        self.assertEqual(done["ledger_rows"][0]["status"], "aborted")
        self.assertEqual(done["ledger_rows"][0]["detail"], {"reason": "duplicate start"})
        self.run_tool("check-run", "--run-id", RUN)

    def start_twice(self, command, id_name, now_a, now_b):
        """Two sessions run `command` from the same exported ledger, then both push and re-export."""
        other = self.root.parent / (self.root.name + "_session_b")
        shutil.copytree(self.root, other)
        self.addCleanup(shutil.rmtree, other, True)
        first = self.run_tool(command, now=now_a)
        mine = self.root
        self.root = other
        second = self.run_tool(command, now=now_b)
        self.root = mine
        self.assertEqual(first[id_name], second[id_name], "neither session can see the other's start")
        self.assertNotEqual(first["nonce"], second["nonce"])
        self.write_ledger(self.rows + first["ledger_rows"] + second["ledger_rows"])
        self.push()
        return first[id_name], first["nonce"], second["nonce"]

    def test_concurrent_starts_that_share_a_run_id(self):
        self.start("2026-11-08", "2026-11-08T00:05:00-08:00")
        self.end("2026-11-08", "2026-11-08T07:00:00-08:00")
        self.seen = {"acme_ml": entry()}
        self.save()
        run_id, winner, loser = self.start_twice("new-run", "run_id", NOW, "2026-11-09T01:00:20-08:00")
        self.assertEqual(run_id, RUN)
        later = "2026-11-09T01:02:00-08:00"
        ok = self.run_tool("check-run", "--run-id", RUN, "--nonce", winner, now=later)
        self.assertEqual((ok["ok"], ok["starts"], ok["owns_run_id"]), (True, 2, True))
        out = self.run_tool("check-run", "--run-id", RUN, "--nonce", loser, expect=5, now=later)
        self.assertEqual((out["ok"], out["owns_run_id"]), (False, False))
        self.run_tool("check-run", "--run-id", RUN, expect=5, now=later)  # no nonce: a shared id stops both
        # The loser stops without ending the winner's run.
        done = self.run_tool("finish-run", "--run-id", RUN, "--nonce", loser, "--aborted", "duplicate start",
                             now=later)
        self.assertEqual((done["ledger_rows"], done["owns_run_id"]), ([], False))
        self.assertFalse(any(r["event"] == "run_finished" and r["run_id"] == RUN for r in self.ledger()))
        self.run_tool("check-run", "--run-id", RUN, "--nonce", winner, now=later)
        self.assertEqual([j["key"] for j in self.run_tool("select", "--run-id", RUN, now=later)["selected"]],
                         ["acme_ml"])
        self.run_tool("new-run", expect=5, now=later)
        mine = self.run_tool("finish-run", "--run-id", RUN, "--nonce", winner, now=later)
        self.assertEqual(mine["ledger_rows"][0]["status"], "done")

    def test_concurrent_fill_starts_that_share_a_fill_id(self):
        self.save()
        fill_id, winner, loser = self.start_twice("fill-start", "fill_id", "2026-11-09T07:00:00-08:00",
                                                  "2026-11-09T07:00:10-08:00")
        self.assertEqual(fill_id, "fill-2026-11-09")
        later = "2026-11-09T07:01:00-08:00"
        self.run_tool("check-run", "--run-id", fill_id, "--nonce", winner, now=later)
        self.run_tool("check-run", "--run-id", fill_id, "--nonce", loser, expect=5, now=later)
        done = self.run_tool("fill-finish", "--fill-id", fill_id, "--nonce", loser, "--aborted", "duplicate start",
                             now=later)
        self.assertEqual(done["ledger_rows"], [])
        self.run_tool("check-run", "--run-id", fill_id, "--nonce", winner, now=later)

    def test_fill_runs_have_their_own_ids_and_guard(self):
        self.start(RUN, "2026-11-09T00:05:00-08:00")  # an active nightly run
        self.save()
        out = self.run_tool("fill-start")
        self.assertEqual(out["fill_id"], "fill-2026-11-09")
        self.assertTrue((self.root / "pipeline" / "fills" / "fill-2026-11-09").is_dir())
        row = out["ledger_rows"][0]
        self.assertEqual((row["run_id"], row["phase"], row["event"]), ("fill-2026-11-09", "run", "started"))
        err = self.run_tool("fill-start", expect=5)
        self.assertEqual(err["active_runs"], ["fill-2026-11-09"])
        self.run_tool("new-run", expect=5)  # blocked by the nightly run, not the fill run
        self.run_tool("finish-run", "--run-id", RUN)
        self.assertEqual(self.run_tool("new-run")["run_id"], "2026-11-09-2")
        done = self.run_tool("fill-finish", "--fill-id", "fill-2026-11-09")
        self.assertEqual(done["ledger_rows"][0]["event"], "run_finished")
        again = self.run_tool("fill-finish", "--fill-id", "fill-2026-11-09")
        self.assertEqual(again["ledger_rows"], [])
        fill = self.run_tool("fill-start")
        self.assertEqual(fill["fill_id"], "fill-2026-11-09-2")
        self.run_tool("check-run", "--run-id", "fill-2026-11-09-2", "--nonce", fill["nonce"], expect=1)  # not pushed
        self.push()
        self.run_tool("check-run", "--run-id", "fill-2026-11-09-2", "--nonce", fill["nonce"])
        self.run_tool("finish-run", "--run-id", "fill-2026-11-09-2", expect=1)


# ---------------------------------------------------------------- lanes

class LaneTest(LedgerCase):
    def setUp(self):
        super().setUp()
        self.start(RUN, "2026-11-09T00:05:00-08:00")

    def test_lanes_reports_documents_states_from_the_ledger(self):
        self.config["lanes"]["max_parallel"] = 2
        for key in ("acme_ml", "globex_ml", "initech_ml", "umbrella_ml", "hooli_ml"):
            self.pick(RUN, key, "2026-11-09T00:20:00-08:00", company=key.split("_")[0].title())
        self.lane(RUN, "acme_ml", "fired", "2026-11-09T00:25:00-08:00")
        self.finished(RUN, "acme_ml", "2026-11-09T00:50:00-08:00")
        self.lane(RUN, "globex_ml", "fired", "2026-11-09T00:30:00-08:00")
        self.lane(RUN, "initech_ml", "fire_failed", "2026-11-09T00:30:00-08:00", detail={"error": "refused"})
        self.save()
        out = self.run_tool("lanes", "--run-id", RUN)
        states = {j["key"]: (j["documents"], j["browser"]) for j in out["jobs"]}
        self.assertEqual(states, {"acme_ml": ("drafted", "ready"), "globex_ml": ("running", "n/a"),
                                  "initech_ml": ("fire_failed", "n/a"), "umbrella_ml": ("queued", "n/a"),
                                  "hooli_ml": ("queued", "n/a")})
        self.assertEqual([m["key"] for m in out["start_documents"]], ["umbrella_ml"])
        self.assertEqual(out["start_documents"][0]["phase"], "documents")
        self.assertEqual(out["start_documents"][0]["pipeline_run"], RUN)
        self.assertEqual(out["start_browser"], [])
        self.assertEqual(out["waiting"], 3)
        self.assertEqual(out["minutes_since_first_lane"], 35)

    def test_mark_fired_appends_fired_or_fire_failed(self):
        self.pick(RUN, "acme_ml", "2026-11-09T00:20:00-08:00")
        self.save()
        out = self.run_tool("mark-fired", "--run-id", RUN, "--key", "acme_ml", "--failed", "trigger refused")
        self.assertEqual((out["fire_count"], out["fire_error"]), (0, "trigger refused"))
        self.assertEqual(self.ledger()[-1]["event"], "fire_failed")
        self.assertEqual(self.ledger()[-1]["detail"], {"error": "trigger refused"})
        out = self.run_tool("mark-fired", "--run-id", RUN, "--key", "acme_ml")
        self.assertEqual((out["fire_count"], out["fire_error"], out["fired_at"]), (1, None, NOW))
        self.assertEqual(out["ledger_rows"][0]["event"], "fired")
        out = self.run_tool("mark-fired", "--run-id", RUN, "--key", "acme_ml", "--phase", "browser")
        self.assertEqual(out["ledger_rows"][0]["phase"], "browser")
        self.run_tool("mark-fired", "--run-id", RUN, "--key", "globex_ml", expect=1)

    def test_write_lane_results_and_browser_gate(self):
        self.pick(RUN, "acme_ml", "2026-11-09T00:20:00-08:00")
        self.pick(RUN, "globex_ml", "2026-11-09T00:20:00-08:00", company="Globex", browser=False)
        self.save()
        running = '{"status": "running"}'
        self.run_tool("write-lane", "--run-id", RUN, "--key", "acme_ml", "--phase", "browser",
                      stdin=running, expect=4)
        absolute = {"status": "drafted", "files": {"cv": "/tmp/cv/main_acme_ml.tex"}}
        err = self.run_tool("write-lane", "--run-id", RUN, "--key", "acme_ml", stdin=json.dumps(absolute), expect=1)
        self.assertIn("working-copy-relative", err["error"])
        self.run_tool("write-lane", "--run-id", RUN, "--key", "acme_ml",
                      stdin='{"status": "drafted", "upload_cv": "x.pdf"}', expect=1)
        result = {"status": "drafted", "company": "Acme", "role": "Machine Learning Engineer",
                  "files": {"cv": "cv/main_acme_ml.tex", "cover_letter": "cover_letters/cover_acme_ml.tex",
                            "answers": "documents/applications/acme_ml/application_form_answers.md",
                            "upload_cv": "Applicant CV.pdf"},
                  "tracker_row": tracker_row("Acme", "Machine Learning Engineer", "https://jobs.example.com/acme_ml")}
        out = self.run_tool("write-lane", "--run-id", RUN, "--key", "acme_ml", stdin=json.dumps(result))
        self.assertEqual(out["written"], f"pipeline/runs/{RUN}/lanes/acme_ml.json")
        saved = json.loads((self.root / out["written"]).read_text(encoding="utf-8"))
        self.assertEqual((saved["status"], saved["pipeline_run"], saved["phase"]), ("drafted", RUN, "documents"))
        row = self.ledger()[-1]
        self.assertEqual((row["event"], row["status"], row["row"]), ("finished", "drafted", None))
        self.assertEqual(row["detail"]["tracker_row"]["company"], "Acme")
        self.run_tool("write-lane", "--run-id", RUN, "--key", "acme_ml", stdin=running, expect=3)
        out = self.run_tool("write-lane", "--run-id", RUN, "--key", "acme_ml", "--phase", "browser", stdin=running)
        self.assertTrue(out["claimed"])
        self.run_tool("write-lane", "--run-id", RUN, "--key", "globex_ml", stdin='{"status": "drafted"}')
        self.run_tool("write-lane", "--run-id", RUN, "--key", "globex_ml", "--phase", "browser",
                      stdin=running, expect=4)

    def test_duplicate_claims(self):
        self.pick(RUN, "acme_ml", "2026-11-09T00:20:00-08:00")
        self.pick(RUN, "globex_ml", "2026-11-09T00:20:00-08:00", company="Globex")
        self.lane(RUN, "acme_ml", "fired", "2026-11-09T00:25:00-08:00")
        self.save()
        args = ("--run-id", RUN, "--key", "acme_ml", "--phase", "documents")
        first = self.run_tool("write-lane", *args, stdin='{"status": "running"}')
        nonce = first["nonce"]
        self.assertRegex(nonce, r"^[0-9a-f]{32}$")
        claim = self.ledger()[-1]
        self.assertEqual((claim["event"], claim["status"], claim["row"], claim["detail"]),
                         ("claimed", "running", None, {"nonce": nonce}))
        self.run_tool("write-lane", *args, stdin='{"status": "running"}', expect=3)
        self.run_tool("check-claim", *args, "--nonce", nonce)
        self.run_tool("check-claim", *args, "--nonce", "0" * 32, expect=3)

        # A second session claimed concurrently and its row reached the Sheet first.
        rows = self.ledger()
        ours = rows[-1]
        number = max(r["row"] for r in rows if r["row"] is not None) + 1
        ours["row"] = number + 1
        rows.append({**ours, "row": number, "at": "2026-11-09T00:59:00-08:00", "detail": {"nonce": "c" * 32}})
        self.write_ledger(rows)
        self.run_tool("check-claim", *args, "--nonce", nonce, expect=3)
        self.run_tool("check-claim", *args, "--nonce", "c" * 32)

        # Rows not pushed yet count after every numbered row, wherever they sit in the file.
        rows = self.ledger()
        globex = {"run_id": RUN, "key": "globex_ml", "phase": "documents", "event": "claimed",
                  "status": "running", "at": "2026-11-09T00:58:00-08:00"}
        rows.insert(0, {**globex, "row": None, "detail": {"nonce": "a" * 32}})
        rows.append({**globex, "row": 40, "detail": {"nonce": "b" * 32}})
        self.write_ledger(rows)
        gargs = ("--run-id", RUN, "--key", "globex_ml", "--phase", "documents")
        self.run_tool("check-claim", *gargs, "--nonce", "a" * 32, expect=3)
        self.run_tool("check-claim", *gargs, "--nonce", "b" * 32)
        self.run_tool("check-claim", "--run-id", RUN, "--key", "acme_ml", "--phase", "browser",
                      "--nonce", nonce, expect=3)

    def test_result_too_large_for_a_sheet_cell_is_refused(self):
        self.pick(RUN, "acme_ml", "2026-11-09T00:20:00-08:00")
        self.save()
        result = {"status": "error", "notes": "x" * 50_000}
        err = self.run_tool("write-lane", "--run-id", RUN, "--key", "acme_ml", stdin=json.dumps(result), expect=1)
        self.assertIn("cell limit", err["error"])
        self.assertFalse((self.root / "pipeline" / "runs" / RUN / "lanes" / "acme_ml.json").exists())
        self.assertEqual(len(self.ledger()), 2)

    def test_global_options_work_after_the_subcommand(self):
        self.pick(RUN, "acme_ml", "2026-11-09T00:20:00-08:00")
        self.save()
        proc = subprocess.run(
            [sys.executable, str(TOOL), "mark-fired", "--run-id", RUN, "--key", "acme_ml",
             "--root", str(self.root), "--now", "2026-11-09T09:15:00Z"],
            capture_output=True, text=True, encoding="utf-8",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["fired_at"], "2026-11-09T01:15:00-08:00")

    def test_claim_survives_a_push(self):
        self.pick(RUN, "acme_ml", "2026-11-09T00:20:00-08:00")
        self.save()
        args = ("--run-id", RUN, "--key", "acme_ml")
        nonce = self.run_tool("write-lane", *args, stdin='{"status": "running"}')["nonce"]
        self.push()
        self.run_tool("check-claim", *args, "--nonce", nonce)


# ---------------------------------------------------------------- fill-queue

class FillQueueTest(LedgerCase):
    FILL_NOW = "2026-11-09T07:30:00-08:00"

    def setUp(self):
        super().setUp()
        a, b, c = "2026-11-07", "2026-11-08", "2026-10-25"
        self.start(a, "2026-11-07T00:05:00-08:00")
        self.pick(a, "acme_ml", "2026-11-07T00:20:00-08:00")
        self.finished(a, "acme_ml", "2026-11-07T01:30:00-08:00")
        self.end(a, "2026-11-07T07:00:00-08:00")
        self.start(b, "2026-11-08T00:05:00-08:00")
        for key, browser in (("globex_ml", True), ("initech_ml", False), ("hooli_ml", True), ("soylent_ml", True)):
            self.pick(b, key, "2026-11-08T00:20:00-08:00", company=key.split("_")[0].title(), browser=browser)
        self.finished(b, "globex_ml", "2026-11-08T01:30:00-08:00")
        self.finished(b, "initech_ml", "2026-11-08T01:35:00-08:00")
        self.finished(b, "hooli_ml", "2026-11-08T01:40:00-08:00", status="error")
        self.finished(b, "soylent_ml", "2026-11-08T01:20:00-08:00")
        self.lane(b, "soylent_ml", "claimed", "2026-11-08T07:40:00-08:00", phase="browser", status="running",
                  detail={"nonce": "d" * 32})  # a browser lane that died yesterday
        self.end(b, "2026-11-08T07:00:00-08:00")
        self.start(c, "2026-10-25T00:05:00-08:00")
        self.pick(c, "umbrella_ml", "2026-10-25T00:20:00-08:00", company="Umbrella")
        self.finished(c, "umbrella_ml", "2026-10-25T01:30:00-08:00")  # older than max_fill_age_days
        self.end(c, "2026-10-25T07:00:00-08:00")

    def queue(self):
        return self.run_tool("fill-queue", now=self.FILL_NOW)

    def test_one_browser_lane_at_a_time(self):
        self.save()
        out = self.queue()
        self.assertEqual([(m["pipeline_run"], m["key"]) for m in out["ready"]],
                         [("2026-11-08", "globex_ml"), ("2026-11-07", "acme_ml")])
        self.assertEqual(out["running"], 0)
        self.assertEqual([m["key"] for m in out["start_browser"]], ["globex_ml"])
        self.assertEqual({m["phase"] for m in out["ready"]}, {"browser"})
        self.assertEqual([h["key"] for h in out["held"]], ["soylent_ml"])

        self.assertEqual(self.run_tool("fill-start", now=self.FILL_NOW)["fill_id"], "fill-2026-11-09")
        self.run_tool("mark-fired", "--run-id", "2026-11-08", "--key", "globex_ml", "--phase", "browser",
                      now=self.FILL_NOW)
        out = self.queue()
        self.assertEqual(out["running"], 1)
        self.assertEqual(out["start_browser"], [])
        self.assertEqual([m["key"] for m in out["ready"]], ["acme_ml"])

        args = ("--run-id", "2026-11-08", "--key", "globex_ml", "--phase", "browser")
        self.run_tool("write-lane", *args, stdin='{"status": "running"}', now="2026-11-09T07:35:00-08:00")
        self.assertEqual(self.queue()["running"], 1)
        self.run_tool("write-lane", *args, stdin='{"status": "filled", "filled": ["name", "email"]}',
                      now="2026-11-09T07:50:00-08:00")
        out = self.run_tool("fill-queue", now="2026-11-09T07:51:00-08:00")
        self.assertEqual(out["running"], 0)
        self.assertEqual([m["key"] for m in out["start_browser"]], ["acme_ml"])

    def test_dead_browser_lane_stops_blocking_the_queue(self):
        self.save()
        args = ("--run-id", "2026-11-08", "--key", "globex_ml", "--phase", "browser")
        self.run_tool("mark-fired", *args, now=self.FILL_NOW)
        self.run_tool("write-lane", *args, stdin='{"status": "running"}', now="2026-11-09T07:35:00-08:00")
        out = self.run_tool("fill-queue", now="2026-11-09T08:00:00-08:00")
        self.assertEqual(out["running"], 1, "a claim inside browser_max_minutes is still running")
        self.assertEqual(out["start_browser"], [])
        out = self.run_tool("fill-queue", now="2026-11-09T08:25:00-08:00")
        self.assertEqual(out["running"], 0, "past browser_max_minutes the lane counts as dead")
        self.assertEqual([m["key"] for m in out["start_browser"]], ["acme_ml"])
        self.assertIn("globex_ml", [h["key"] for h in out["held"]], "the dead lane's job waits for the applicant")

    def test_parallel_browser_setting_and_age_limit(self):
        self.config["lanes"]["max_parallel_browser"] = 2
        self.save()
        out = self.queue()
        self.assertEqual([m["key"] for m in out["start_browser"]], ["globex_ml", "acme_ml"])
        out = self.run_tool("fill-queue", "--max-age-days", "20", now=self.FILL_NOW)
        self.assertEqual([m["key"] for m in out["ready"]], ["globex_ml", "acme_ml", "umbrella_ml"])

    def test_stale_fire_is_retried_until_the_attempt_limit(self):
        self.lane("2026-11-08", "globex_ml", "fired", "2026-11-08T07:45:00-08:00", phase="browser")
        self.save()
        out = self.queue()
        self.assertIn("globex_ml", [m["key"] for m in out["ready"]], "one stale fire leaves a retry")
        self.lane("2026-11-08", "globex_ml", "fired", "2026-11-08T08:00:00-08:00", phase="browser")
        self.write_ledger(self.rows)
        out = self.queue()
        self.assertNotIn("globex_ml", [m["key"] for m in out["ready"]])
        self.assertIn("globex_ml", [h["key"] for h in out["held"]])


    def test_a_job_drafted_on_two_runs_is_offered_once(self):
        b = "2026-11-08"
        self.pick(b, "acme_ml", "2026-11-08T00:20:00-08:00", attempt=2)  # a retry of 2026-11-07's lane
        self.finished(b, "acme_ml", "2026-11-08T02:00:00-08:00")
        self.save()
        out = self.queue()
        self.assertEqual([(m["pipeline_run"], m["key"]) for m in out["ready"]],
                         [(b, "acme_ml"), (b, "globex_ml")], "once, from the newest draft")
        # A live lane on either run keeps the other run's draft out of the queue.
        self.lane("2026-11-07", "acme_ml", "fired", "2026-11-09T07:20:00-08:00", phase="browser")
        self.write_ledger(self.rows)
        out = self.queue()
        self.assertEqual([m["key"] for m in out["ready"]], ["globex_ml"])
        self.assertEqual(out["running"], 1)
        # Once filled from one run, it is not filled again from the other.
        self.finished("2026-11-07", "acme_ml", "2026-11-09T07:25:00-08:00", status="filled", phase="browser")
        self.write_ledger(self.rows)
        out = self.queue()
        self.assertEqual(([m["key"] for m in out["ready"]], out["running"]), (["globex_ml"], 0))

    def test_jobs_the_tracker_shows_as_applied_or_withdrawn_are_held(self):
        self.tracker_text = ",".join(TRACKER_HEADER) + "\n" + "".join(
            f"{d},{c},Software,Machine Learning Engineer,ml,portal,{st},,78,,,,{u},\n" for d, c, st, u in (
                ("2025-02-01", "Acme", "rejected", "https://jobs.example.com/acme_2025"),  # an earlier application
                ("2026-11-08", "Globex", "applied", "https://jobs.example.com/globex_ml"),
                ("2026-11-08", "Soylent", "withdrawn", "https://jobs.example.com/other")))
        self.save()
        out = self.queue()
        self.assertTrue(out["tracker_checked"])
        self.assertNotIn("warning", out)
        self.assertEqual([m["key"] for m in out["ready"]], ["acme_ml"])
        held = {h["key"]: h["reason"] for h in out["held"]}
        self.assertIn("applied", held["globex_ml"])
        self.assertIn("soylent_ml", held)  # its stale claim is reported first

    def test_a_missing_tracker_is_reported_not_silently_skipped(self):
        self.tracker_text = ",".join(TRACKER_HEADER) + "\n" + (
            "2026-11-08,Globex,Software,Machine Learning Engineer,ml,portal,applied,,78,,,,"
            "https://jobs.example.com/globex_ml,\n")
        self.save()
        (self.root / "job_search_tracker.csv").unlink()
        out = self.queue()
        self.assertFalse(out["tracker_checked"])
        self.assertIn("job_search_tracker.csv", out["warning"])
        self.assertIn("not checked", out["warning"])
        # Without the tracker an already-applied job can't be held: it is offered again.
        self.assertEqual([m["key"] for m in out["ready"]], ["globex_ml", "acme_ml"])
        self.assertFalse((self.root / "job_search_tracker.csv").exists(), "fill-queue only reads")

    def test_a_draft_naming_absolute_paths_is_held(self):
        self.finished("2026-11-08", "globex_ml", "2026-11-08T03:00:00-08:00",
                      files={"cv": "/srv/work/cv/main_globex_ml.tex"})
        self.save()
        out = self.queue()
        self.assertEqual([m["key"] for m in out["ready"]], ["acme_ml"])
        held = {h["key"]: h["reason"] for h in out["held"]}
        self.assertIn("working-copy-relative", held["globex_ml"])


# ---------------------------------------------------------------- record and stats

class RecordTest(LedgerCase):
    def test_record_picks_up_late_drafts_from_earlier_runs(self):
        old_header = ",".join(h for h in TRACKER_HEADER if h != "deadline")
        self.tracker_text = (old_header + "\n" + "2026-11-01,Initech,Software,Data Engineer,data,portal,applied,,"
                             "70,,,,https://jobs.example.com/initech_data-engineer\n")
        early = "2026-11-07"
        self.start(early, "2026-11-07T00:05:00-08:00")
        self.pick(early, "acme_ml", "2026-11-07T00:20:00-08:00")
        self.lane(early, "acme_ml", "fired", "2026-11-07T00:25:00-08:00")
        self.end(early, "2026-11-07T07:00:00-08:00")
        self.finished(early, "acme_ml", "2026-11-07T09:00:00-08:00",  # after its run reported
                      trow=tracker_row("Acme", "Machine Learning Engineer", "https://jobs.example.com/acme_ml"))
        self.start(RUN, "2026-11-09T00:05:00-08:00")
        for key, company in (("globex_ds", "Globex"), ("initech_data-engineer", "Initech"), ("hooli_ml", "Hooli")):
            self.pick(RUN, key, "2026-11-09T00:20:00-08:00", company=company)
        self.finished(RUN, "globex_ds", "2026-11-09T00:40:00-08:00",
                      trow=tracker_row("Globex", "Data Scientist", "https://jobs.example.com/globex_ds"))
        self.finished(RUN, "initech_data-engineer", "2026-11-09T00:45:00-08:00",
                      trow=tracker_row("Initech", "Data Engineer", "https://jobs.example.com/initech_data-engineer"))
        self.finished(RUN, "hooli_ml", "2026-11-09T00:50:00-08:00", status="error")
        self.save()
        tracker = self.root / "job_search_tracker.csv"

        dry = self.run_tool("record", "--dry-run")
        self.assertEqual([a["action"] for a in dry["actions"]], ["would append", "would append", "skipped"])
        self.assertEqual(dry["ledger_rows"], [])
        self.assertEqual(tracker.read_text(encoding="utf-8"), self.tracker_text)
        self.assertEqual(self.ledger(), self.rows)

        out = self.run_tool("record")
        self.assertEqual([(a["run"], a["key"], a["action"]) for a in out["actions"]],
                         [(early, "acme_ml", "appended"), (RUN, "globex_ds", "appended"),
                          (RUN, "initech_data-engineer", "skipped")])
        self.assertEqual([r["company"] for r in out["added"]], ["Acme", "Globex"])
        self.assertEqual((out["already_in_tracker"], out["already_recorded"]), (1, 0))
        self.assertEqual([(r["row"], r["run_id"], r["key"], r["phase"], r["event"], r["detail"])
                          for r in out["ledger_rows"]],
                         [(None, early, "acme_ml", "documents", "recorded",
                           {"company": "Acme", "role": "Machine Learning Engineer"}),
                          (None, RUN, "globex_ds", "documents", "recorded",
                           {"company": "Globex", "role": "Data Scientist"})])
        self.assertEqual(self.ledger()[-2:], out["ledger_rows"])
        with tracker.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
        self.assertEqual(reader.fieldnames, TRACKER_HEADER)
        self.assertEqual([(r["company"], r["status"]) for r in rows],
                         [("Initech", "applied"), ("Acme", "drafted"), ("Globex", "drafted")])

        again = self.run_tool("record")
        self.assertEqual((again["added"], again["already_in_tracker"], again["already_recorded"]), ([], 1, 2))
        self.assertEqual(again["ledger_rows"], [])
        alias = self.run_tool("append-tracker", "--all")
        self.assertEqual((alias["added"], alias["already_in_tracker"], alias["already_recorded"]), ([], 1, 2))
        with tracker.open(encoding="utf-8", newline="") as fh:
            self.assertEqual(len(list(csv.DictReader(fh))), 3)

    def test_record_never_brings_back_a_row_the_applicant_deleted(self):
        night = "2026-11-08"
        self.start(night, "2026-11-08T00:05:00-08:00")
        self.pick(night, "acme_ml", "2026-11-08T00:20:00-08:00")
        self.finished(night, "acme_ml", "2026-11-08T01:00:00-08:00",
                      trow=tracker_row("Acme", "Machine Learning Engineer", "https://jobs.example.com/acme_ml"))
        self.save()
        self.assertEqual(len(self.run_tool("record")["added"]), 1)
        self.push()
        tracker = self.root / "job_search_tracker.csv"
        tracker.write_text(",".join(TRACKER_HEADER) + "\n", encoding="utf-8")  # the applicant deleted it
        again = self.run_tool("record")
        self.assertEqual((again["added"], again["already_recorded"], again["ledger_rows"]), ([], 1, []))
        self.assertEqual(tracker.read_text(encoding="utf-8"), ",".join(TRACKER_HEADER) + "\n")

    def test_a_closed_row_for_the_same_role_does_not_block_a_new_draft(self):
        self.tracker_text = (",".join(TRACKER_HEADER) + "\n" + "2025-03-01,Bravo,Software,Data Scientist,ds,"
                             "portal,rejected,,70,,,,https://jobs.example.com/bravo_old,\n")
        night = "2026-10-20"
        self.start(night, "2026-10-20T00:05:00-07:00")
        self.pick(night, "bravo_ds", "2026-10-20T00:20:00-07:00", company="Bravo", title="Data Scientist")
        self.finished(night, "bravo_ds", "2026-10-20T01:00:00-07:00",
                      trow=tracker_row("Bravo", "Data Scientist", "https://jobs.example.com/bravo_ds"))
        self.save()
        out = self.run_tool("record")
        self.assertEqual([a["action"] for a in out["actions"]], ["appended"])
        with (self.root / "job_search_tracker.csv").open(encoding="utf-8", newline="") as fh:
            self.assertEqual([r["status"] for r in csv.DictReader(fh)], ["rejected", "drafted"])
        self.push()
        stats = self.run_tool("stats")
        self.assertEqual(stats["drafts_older_than_min_age"],
                         {"submitted": 0, "not_submitted": 1, "withdrawn": 0, "missing_from_tracker": 0})

    def test_stats_from_ledger_and_tracker(self):
        old = "2026-10-20"
        self.start(old, "2026-10-20T00:05:00-07:00")
        for key, company in (("acme_ml", "Acme"), ("globex_ds", "Globex"), ("umbrella_ml", "Umbrella")):
            self.pick(old, key, "2026-10-20T00:20:00-07:00", company=company)
        self.finished(old, "acme_ml", "2026-10-20T01:00:00-07:00",
                      trow=tracker_row("Acme", "Machine Learning Engineer", "https://jobs.example.com/acme_ml"))
        self.recorded(old, "acme_ml", "2026-10-20T07:00:00-07:00", "Acme", "Machine Learning Engineer")
        self.finished(old, "acme_ml", "2026-10-20T08:00:00-07:00", status="filled", phase="browser")
        self.finished(old, "globex_ds", "2026-10-20T01:05:00-07:00",
                      trow=tracker_row("Globex", "Data Scientist", "https://jobs.example.com/globex_ds"))
        self.recorded(old, "globex_ds", "2026-10-20T07:00:00-07:00", "Globex", "Data Scientist")
        self.finished(old, "umbrella_ml", "2026-10-20T01:10:00-07:00", status="error")
        self.start("2026-11-08", "2026-11-08T00:05:00-08:00")
        self.pick("2026-11-08", "initech_ml", "2026-11-08T00:20:00-08:00", company="Initech")
        self.finished("2026-11-08", "initech_ml", "2026-11-08T01:00:00-08:00",
                      trow=tracker_row("Initech", "Machine Learning Engineer", "https://jobs.example.com/initech_ml"))
        self.recorded("2026-11-08", "initech_ml", "2026-11-08T07:00:00-08:00", "Initech", "Machine Learning Engineer")
        self.tracker_text = ",".join(TRACKER_HEADER) + "\n" + "".join(
            f"2026-10-20,{c},Software,{r},ml,portal,{s},,78,,,,https://jobs.example.com/{k},\n"
            for c, r, s, k in (("Acme", "Machine Learning Engineer", "applied", "acme_ml"),
                               ("Globex", "Data Scientist", "drafted", "globex_ds"),
                               ("Initech", "Machine Learning Engineer", "drafted", "initech_ml")))
        self.save()
        out = self.run_tool("stats")
        self.assertEqual(out["drafts_older_than_min_age"],
                         {"submitted": 1, "not_submitted": 1, "withdrawn": 0, "missing_from_tracker": 0})
        self.assertEqual(out["drafts_too_recent_to_judge"], 1)
        self.assertEqual(out["lane_outcomes_all_runs"], {"filled": 1, "drafted, not filled": 2, "error": 1})
        self.assertEqual(out["line"], "Pick quality: 1 of 2 pipeline drafts older than 7 days were submitted (50%).")


# ---------------------------------------------------------------- check

class CheckTest(LedgerCase):
    def setUp(self):
        super().setUp()
        for rel in (".claude/skills/job-scraper/SKILL.md", ".claude/commands/rank.md",
                    ".claude/commands/apply.md", "pipeline/skills/form-answers/SKILL.md"):
            path = self.root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("stub\n", encoding="utf-8")

    def test_valid_config_passes_and_hides_ids(self):
        self.save()
        out = self.run_tool("check")
        self.assertTrue(out["ok"], out["problems"])
        self.assertEqual(out["warnings"], [])
        self.assertEqual(out["ledger"], {"rows": 0, "unpushed": 0})
        self.assertEqual(out["lanes"]["browser_check_minutes"], 10)
        self.assertEqual(out["lanes"]["max_fill_age_days"], 7)
        for secret in ("TASK_ID_DOCUMENTS", "TASK_ID_BROWSER", "SHEET_ID_TEST", "FOLDER_ID_ROOT"):
            self.assertNotIn(secret, self.last_stdout)

    def test_old_task_id_is_an_alias_with_a_warning(self):
        lanes = self.config["lanes"]
        lanes["task_id"] = lanes.pop("documents_task_id")
        self.save()
        out = self.run_tool("check")
        self.assertTrue(out["ok"], out["problems"])
        self.assertEqual(len(out["warnings"]), 1)
        self.assertIn("documents_task_id", out["warnings"][0])

    def test_problems_are_reported(self):
        self.config["drive"]["overlay"].append("/tmp/CLAUDE.md")
        self.config["drive"]["replaceable"].append("../outside/*.json")
        self.config["drive"]["sheet_id"] = "SPREADSHEET_ID"
        self.config["lanes"]["documents_task_id"] = "TASK_ID"
        self.config["lanes"]["max_fill_age_days"] = 0
        self.save()
        out = self.run_tool("check", expect=1)
        text = "\n".join(out["problems"])
        for needle in ("drive.overlay entry '/tmp/CLAUDE.md'", "drive.replaceable entry '../outside/*.json'",
                       "drive.sheet_id is still the placeholder", "lanes.documents_task_id is still the placeholder",
                       "lanes.max_fill_age_days"):
            self.assertIn(needle, text)
        del self.config["drive"]
        self.save_config()
        out = self.run_tool("check", expect=1)
        self.assertTrue(any(p.startswith("drive:") for p in out["problems"]))

    def test_malformed_ledger_is_a_problem(self):
        self.save()
        (self.root / "pipeline" / "ledger.json").write_text('{"rows": [{"row": "two"}]}', encoding="utf-8")
        out = self.run_tool("check", expect=1)
        self.assertTrue(any("ledger.json" in p for p in out["problems"]))


# ---------------------------------------------------------------- ledger-from-runs

class LedgerFromRunsTest(LedgerCase):
    def write(self, path: Path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=1) if not isinstance(data, str) else data, encoding="utf-8")

    def old_job(self, key, company, **stamps):
        job = {"key": key, "title": "Machine Learning Engineer", "company": company,
               "url": f"https://jobs.example.com/{key}", "rank_score": 80, "rank_verdict": "Strong Fit",
               "deadline": None, "posted_date": "2026-09-20", "portal": "example-portal", "attempt": 1,
               "fired_at": None, "fire_count": 0, "browser_fired_at": None, "browser_fire_count": 0,
               "recorded": False}
        job.update(stamps)
        return job

    def build_old_runs(self) -> Path:
        old = self.root / "old_runs"
        one = old / "2026-10-01"
        self.write(one / "run.json", {"run_id": "2026-10-01", "started_at": "2026-10-01T00:00:05-07:00",
                                      "timezone": "America/Los_Angeles"})
        self.write(one / "manifest.json", {
            "run_id": "2026-10-01", "created_at": "2026-10-01T00:20:00-07:00", "browser_enabled": True,
            "browser_off_reason": None, "skipped": [], "counts": {"selected": 3},
            "jobs": [
                self.old_job("acme_ml-engineer", "Acme", fired_at="2026-10-01T00:25:00-07:00", fire_count=1),
                self.old_job("globex_ml-engineer", "Globex", fire_error="trigger refused"),
                self.old_job("initech_ml-engineer", "Initech", fired_at="2026-10-01T00:26:00-07:00", fire_count=1,
                             browser_fired_at="2026-10-01T07:40:00-07:00", browser_fire_count=1,
                             recorded=True, tracker_company="Initech", tracker_role="Machine Learning Engineer"),
            ]})
        self.write(one / "lanes" / "acme_ml-engineer.json", {
            "status": "error", "reason": "posting closed", "key": "acme_ml-engineer",
            "pipeline_run": "2026-10-01", "phase": "documents", "updated_at": "2026-10-01T00:50:00-07:00"})
        self.write(one / "lanes" / "initech_ml-engineer.json", {
            "status": "drafted", "company": "Initech", "role": "Machine Learning Engineer",
            "files": {"cv": "cv/main_initech_ml-engineer.tex"},
            "tracker_row": tracker_row("Initech", "Machine Learning Engineer",
                                       "https://jobs.example.com/initech_ml-engineer"),
            "key": "initech_ml-engineer", "pipeline_run": "2026-10-01", "phase": "documents",
            "updated_at": "2026-10-01T01:10:00-07:00"})
        self.write(one / "lanes" / "initech_ml-engineer.browser.json", {
            "status": "filled", "key": "initech_ml-engineer", "pipeline_run": "2026-10-01", "phase": "browser",
            "updated_at": "2026-10-01T07:55:00-07:00"})
        self.write(one / "lanes" / "stray_job.json", {"status": "error"})
        self.write(one / "report.md", "# Report\n")
        two = old / "2026-10-02"
        self.write(two / "run.json", {"run_id": "2026-10-02", "started_at": "2026-10-02T00:00:05-07:00",
                                      "timezone": "America/Los_Angeles"})
        self.write(two / "manifest.json", {
            "run_id": "2026-10-02", "created_at": "2026-10-02T00:20:00-07:00", "browser_enabled": False,
            "browser_off_reason": "Chrome not reachable", "skipped": [], "counts": {"selected": 1},
            "jobs": [self.old_job("acme_ml-engineer", "Acme", attempt=2, fired_at="2026-10-02T00:25:00-07:00",
                                  fire_count=1)]})
        self.write(two / "lanes" / "acme_ml-engineer.json", {
            "status": "running", "key": "acme_ml-engineer", "pipeline_run": "2026-10-02",
            "phase": "documents", "updated_at": "2026-10-02T00:30:00-07:00"})
        self.write(old / "_probe" / "target.json", "{}")
        return old

    def test_ledger_from_runs_on_old_run_folders(self):
        old = self.build_old_runs()
        self.seen = {k: entry(company=c, url=f"https://jobs.example.com/{k}") for k, c in (
            ("acme_ml-engineer", "Acme"), ("globex_ml-engineer", "Globex"), ("initech_ml-engineer", "Initech"))}
        self.save()
        (self.root / "pipeline" / "ledger.json").unlink()  # a first migration starts without a ledger

        out = self.run_tool("ledger-from-runs", "--runs-dir", str(old))
        self.assertEqual(out["migrated_runs"], ["2026-10-01", "2026-10-02"])
        self.assertEqual(out["skipped_runs"], [{"run": "_probe", "reason": "not a run folder name"}])
        self.assertTrue(any("stray_job.json" in w for w in out["warnings"]))
        rows = self.ledger()
        self.assertEqual(out["rows_added"], len(rows))
        A, G, I = "acme_ml-engineer", "globex_ml-engineer", "initech_ml-engineer"
        one, two = "2026-10-01", "2026-10-02"
        self.assertEqual([(r["run_id"], r["key"], r["phase"], r["event"], r["status"]) for r in rows], [
            (one, "", "run", "started", ""),
            (one, A, "documents", "selected", ""),
            (one, G, "documents", "selected", ""),
            (one, I, "documents", "selected", ""),
            (one, G, "documents", "fire_failed", ""),
            (one, A, "documents", "fired", ""),
            (one, I, "documents", "fired", ""),
            (one, A, "documents", "claimed", "running"),
            (one, A, "documents", "finished", "error"),
            (one, I, "documents", "claimed", "running"),
            (one, I, "documents", "finished", "drafted"),
            (one, I, "documents", "recorded", ""),
            (one, I, "browser", "fired", ""),
            (one, I, "browser", "claimed", "running"),
            (one, I, "browser", "finished", "filled"),
            (one, "", "run", "run_finished", "done"),
            (two, "", "run", "started", ""),
            (two, A, "documents", "selected", ""),
            (two, A, "documents", "fired", ""),
            (two, A, "documents", "claimed", "running"),
            (two, "", "run", "run_finished", "aborted"),
        ])
        self.assertTrue(all(r["row"] is None for r in rows))
        moments = [datetime.fromisoformat(r["at"]) for r in rows]
        self.assertEqual(moments, sorted(moments), "rows come in time order")
        self.assertEqual(rows[1]["detail"], {"url": f"https://jobs.example.com/{A}", "title": "Machine Learning Engineer",
                                             "company": "Acme", "rank_score": 80, "rank_verdict": "Strong Fit",
                                             "attempt": 1, "browser": False},
                         "migrated picks never go to the morning filler")
        self.assertIs(rows[17]["detail"]["browser"], False)
        self.assertEqual(rows[4]["detail"], {"error": "trigger refused"})
        self.assertEqual(rows[10]["detail"]["tracker_row"]["company"], "Initech")
        self.assertEqual(rows[11]["detail"], {"company": "Initech", "role": "Machine Learning Engineer"})

        # The old "recorded" flag carries over: record never brings back a row the applicant deleted.
        rec = self.run_tool("record")
        self.assertEqual((rec["added"], rec["already_recorded"]), ([], 1))
        self.assertEqual(self.run_tool("fill-queue")["ready"], [])

        again = self.run_tool("ledger-from-runs", "--runs-dir", str(old))
        self.assertEqual(again["rows_added"], 0)
        self.assertEqual([s["reason"] for s in again["skipped_runs"] if s["run"] != "_probe"],
                         ["already in the ledger", "already in the ledger"])
        self.assertEqual(len(self.ledger()), len(rows))

        # Retries and "already handled" carry over into the first run on the ledger.
        self.assertEqual(self.run_tool("new-run")["run_id"], RUN)
        out = self.run_tool("select", "--run-id", RUN)
        self.assertEqual([(j["key"], j["attempt"]) for j in out["selected"]], [(G, 1)])
        self.assertEqual(self.reasons(out), {A: "already handled on run 2026-10-02 (unfinished)",
                                             I: "already handled on run 2026-10-01 (drafted)"})

    def test_dry_run_writes_nothing(self):
        old = self.build_old_runs()
        self.save()
        out = self.run_tool("ledger-from-runs", "--runs-dir", str(old), "--dry-run")
        self.assertEqual(out["rows_added"], 0)
        self.assertEqual(out["rows_found"], 21)
        self.assertEqual(self.ledger(), [])

    def test_lane_result_too_long_for_a_sheet_cell_is_slimmed(self):
        old = self.root / "old_runs"
        run = old / "2026-10-06"
        self.write(run / "run.json", {"run_id": "2026-10-06", "started_at": "2026-10-06T00:00:05-07:00"})
        self.write(run / "manifest.json", {"run_id": "2026-10-06", "created_at": "2026-10-06T00:20:00-07:00",
                                           "browser_enabled": True,
                                           "jobs": [self.old_job("acme_ml", "Acme",
                                                                 fired_at="2026-10-06T00:25:00-07:00")]})
        self.write(run / "lanes" / "acme_ml.json", {
            "status": "drafted", "company": "Acme", "role": "Machine Learning Engineer",
            "questions": ["q" * 1000] * 60, "files": {"cv": "cv/main_acme_ml.tex"},
            "key": "acme_ml", "pipeline_run": "2026-10-06", "phase": "documents",
            "updated_at": "2026-10-06T01:00:00-07:00"})
        self.save()
        out = self.run_tool("ledger-from-runs", "--runs-dir", str(old))
        self.assertTrue(any("Sheet cell" in w for w in out["warnings"]))
        fin = next(r for r in self.ledger() if r["event"] == "finished")
        self.assertNotIn("questions", fin["detail"])
        self.assertEqual((fin["detail"]["status"], fin["detail"]["files"]), ("drafted", {"cv": "cv/main_acme_ml.tex"}))
        size = len(json.dumps(fin["detail"], sort_keys=True, ensure_ascii=False, separators=(",", ":"))) + 1
        self.assertLessEqual(size, 50_000)

    def test_unreadable_lane_result_is_migrated_as_an_error(self):
        old = self.root / "old_runs"
        run = old / "2026-10-05"
        self.write(run / "run.json", {"run_id": "2026-10-05", "started_at": "2026-10-05T00:00:05-07:00"})
        self.write(run / "manifest.json", {"run_id": "2026-10-05", "created_at": "2026-10-05T00:20:00-07:00",
                                           "browser_enabled": True,
                                           "jobs": [self.old_job("acme_ml", "Acme",
                                                                 fired_at="2026-10-05T00:25:00-07:00")]})
        self.write(run / "lanes" / "acme_ml.json", "{not json")
        self.save()
        out = self.run_tool("ledger-from-runs", "--runs-dir", str(old))
        self.assertTrue(any("acme_ml.json" in w for w in out["warnings"]))
        finished = [r for r in self.ledger() if r["event"] == "finished"]
        self.assertEqual([(r["key"], r["status"]) for r in finished], [("acme_ml", "error")])
        self.assertEqual(self.ledger()[-1]["status"], "aborted", "a run without report.md is closed as aborted")


# ---------------------------------------------------------------- pipeline skills

class PipelineSkillsFrontmatterTest(unittest.TestCase):
    def test_every_pipeline_skill_has_name_and_description(self):
        skills = sorted((REPO / "pipeline" / "skills").glob("*/SKILL.md"))
        if not skills:
            self.skipTest("no pipeline/skills/*/SKILL.md yet")
        try:
            import yaml
        except ImportError:
            self.skipTest("PyYAML is not installed")
        for path in skills:
            with self.subTest(skill=path.parent.name):
                text = path.read_text(encoding="utf-8")
                self.assertTrue(text.startswith("---\n"), "SKILL.md must start with YAML frontmatter")
                end = text.find("\n---", 4)
                self.assertNotEqual(end, -1, "unterminated frontmatter")
                data = yaml.safe_load(text[4:end])
                self.assertIsInstance(data, dict)
                for field in ("name", "description"):
                    self.assertIsInstance(data.get(field), str, f"{field} missing")
                    self.assertTrue(data[field].strip(), f"{field} is empty")


if __name__ == "__main__":
    unittest.main()
