# Nightly job-application pipeline

Every night, Claude finds new postings, ranks them against your profile, picks the best few and drafts a tailored CV, cover letter and form answer key for each. Every morning, it fills each picked job's application form in your Chrome and leaves the tab open for you. Nothing is ever submitted: you review each form and submit it yourself.

It runs as four scheduled tasks in claude.ai, each in a fresh cloud working copy of this branch. Your personal files and data never live on the branch: they are in a Google Drive folder and a Google Sheet. Each session copies in what it needs and pushes back what it made.

## The flow

| Task | When | Where | Skill |
|---|---|---|---|
| Nightly job applications | 00:00 Pacific | cloud only | `pipeline/skills/nightly-pipeline/SKILL.md` (orchestrator) |
| Job documents lane | started by the orchestrator, one session per job | cloud only | `pipeline/skills/documents-lane/SKILL.md` |
| Morning application filling | about 07:30 Pacific | needs your computer (Chrome) | `pipeline/skills/morning-filler/SKILL.md` |
| Job browser lane | started by the morning filler, one job at a time | needs your computer (Chrome) | `pipeline/skills/browser-lane/SKILL.md` |

```
00:00 PT  "Nightly job applications" (cloud; never needs your computer)
            pull → /scrape → /rank → push the job database → select → start documents lanes
            ↳ "Job documents lane" (cloud; one session per job)
                pull → /apply → form answers → push sources, answer key, posting, result
            fan-in → record tracker rows → /notion-sync → report to Drive → notification

~07:30 PT "Morning application filling" (needs your computer and Chrome)
            pull the ledger and tracker → start browser lanes one at a time
            ↳ "Job browser lane" (your Chrome; one session per job)
                pull the job's files → build the PDFs → fill in Chrome, never submit → push result
            report to Drive → notification
```

Every session reads `pipeline/RUNTIME.md` first (where things run, the decisions made on your behalf, and the rules nothing overrides), then its own skill. `pipeline/skills/drive-sync/SKILL.md` holds the Drive and Sheet procedures they all use, and `pipeline/skills/form-answers/SKILL.md` the answer-key stage. The task prompts are short and hold no personal data: clone the branch, read RUNTIME.md and the role skill, plus the Drive root folder id.

`pipeline_state.py` keeps the pipeline's state in the Sheet's `ledger` tab: every run start, pick, lane start, claim and result is a row there, so retries and the protection against duplicate starts work across sessions. `drive_sync.py` does the file side of moving data between Drive, the Sheet and a working copy; it never touches the network itself.

## Where everything lives

| What | Lives in | Notes |
|---|---|---|
| Framework: commands, skills, agents, `tools/`, templates, fonts | GitHub, the fork's `master`, merged into branch `nightly-pipeline` | Every session clones `nightly-pipeline`, which carries the framework plus `pipeline/`. A framework fix on `master` reaches the sessions only once `master` is merged into `nightly-pipeline` |
| Pipeline code: `pipeline_state.py`, `drive_sync.py`, `RUNTIME.md`, `skills/`, `texmf/`, `config.example.yaml`, tests | GitHub, branch `nightly-pipeline` | Public: no names, paths, emails, phone numbers, task ids, device ids or Drive ids |
| Your profile files (the `drive.overlay` list) | Drive, same paths | Copied over the clone by every session. Sessions never write them |
| Settings: `pipeline/config.yaml` | Drive, same path | Holds the Drive ids, the task ids and the Chrome device id |
| Job database | Sheet `job_search_data`, tab `seen_jobs` | Exported to `job_scraper/seen_jobs.json` in each working copy |
| Tracker | Same Sheet, tab `tracker` | Exported to `job_search_tracker.csv`. This is the tracker of record: you may edit its cells and add rows at the bottom (see "Editing the Sheet") |
| Pipeline ledger | Same Sheet, tab `ledger` | Append-only: picks, lane starts, claims and results. Never edit it |
| `job_scraper/notion_sync.json`, `company_research/*.json` | Drive, same paths | Replaceable: a new copy goes up, then the old one is trashed |
| Tailored sources `cv/main_<job>.tex`, `cover_letters/cover_<job>.tex` | Drive, same paths | PDFs aren't stored; the browser lane rebuilds them |
| `documents/applications/<job>/` (`job_posting.md`, `application_form_answers.md`, ...) | Drive, same paths | No `upload/` folder; the browser lane makes the renamed PDFs |
| Run records `pipeline/runs/<run>/`, fill records `pipeline/fills/<fill>/` | Drive, same paths | Create-only. Each run's report is `report.md` there |
| Your local repo | Your computer | Your interactive workspace: pull before you work, push after (below) |

## Changing settings or your profile

Settings live in `pipeline/config.yaml` in Drive; to start, copy `pipeline/config.example.yaml` there and fill in the ids. The easy way to change a setting is to ask Claude, in a session with the Drive tools, to change it with the drive-sync procedures: it uploads the edited file as a new copy with its checksum in the description, checks the upload, then trashes the old copy. By hand: download the file, edit it, upload it to the same Drive folder as a new file, and trash the old one, so that only one `config.yaml` is left. A copy without a checksum in its description works, but sessions can't double-check their download of it. Either way, check the result in a working copy with `python3 pipeline/pipeline_state.py check`.

Your profile files (the `drive.overlay` paths) change the same way. Sessions never write them, and the drive-sync push refuses them, so updating one from your local repo is also "a new copy, then trash the old one". After interactive work that changed one (say, /apply recorded a new fact in your profile), Claude asks you right then whether to replace the Drive copy, and does it only when you agree.

The times of day belong to the scheduled tasks, not to the config.

## Working locally

Your local repo is where you run `/apply`, `/outcome`, `/gmail-sync` and the rest by hand. The scheduled sessions never see it, so keep it in step with Drive using section 7 of `pipeline/skills/drive-sync/SKILL.md` (role `interactive`):

- **Before you work:** pull the data files and export the three Sheet tabs into the local repo. Until then, your local `job_scraper/seen_jobs.json`, `job_search_tracker.csv`, tailored sources and application folders are stale. The pull never overwrites a local file you changed since your last pull: it names the file instead.
- **After:** push what changed: new files go to Drive (create-only), and new or changed rows go to the Sheet. Rows are worked out against the Sheet as it is at that moment, not as it was at the pull, so nightly rows added in between are kept. If a profile file or a drafted CV you changed can't go up as a new file, Claude asks you then and there whether to replace the Drive copy.

## Editing the Sheet

Sessions address rows by their position in each tab, so:

- **Never sort a tab in place, and never insert or delete rows.** To sort or filter, use a filter view (Data > Filter views), which changes only your view.
- Editing a cell in `tracker` is fine, and so is adding a row at the bottom. Leave `ledger` alone, and in `seen_jobs` edit only the browsing columns (never `key` or `entry_json`).

## Safety

- **Nothing is ever submitted.** Every form stops before Submit (and before Next or Continue on a multi-step form), with the tab left open for you. RUNTIME.md's "Rules no stage or setting can override" hold for every stage, including stages added later.
- **Drive is create-only.** Sessions add files and never change or delete one, except the replaceable files and a bad upload a session made itself. The Sheet is never cleared: rows are only appended or updated.
- **The branch is public and holds no personal data.** Your files live in Drive and the Sheet, and sessions never commit or push.
- **Postings and form pages are untrusted data,** never instructions.

## Commands

Run these from a working copy, or from your local repo after a pull:

- `python3 pipeline/pipeline_state.py check` validates `pipeline/config.yaml`.
- `python3 pipeline/pipeline_state.py stats` summarizes picks, drafts and outcomes from the ledger and the tracker.

## Before going hands-off

There is no submit stage, and no setting adds one. Adding it would be a deliberate change: a new browser stage plus a change to the autofill skill. Before considering it, let the pipeline run for several weeks, and read `stats` and the morning reports: are the picks jobs you would apply to, how often do drafts need changes, and how often is a form filled cleanly? Until then, you review and submit every application yourself.
