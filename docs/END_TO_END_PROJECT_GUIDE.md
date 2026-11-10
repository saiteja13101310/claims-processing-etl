# Claims Processing ETL — End-to-End Project Guide

**Project:** claims-processing-etl  
**Repo:** https://github.com/saiteja13101310/claims-processing-etl  
**Workspace:** dbc-bba69e81-8b26.cloud.databricks.com  
**Last updated:** 2026-11-10 (dev phase complete; prod phase pending)

This document tells the complete story of the project: what was built, how it
was set up, every problem that was hit and how it was fixed, what the current
state is, and exactly what remains for production. A reader with no prior
context should be able to understand and continue this project from here.

For day-to-day reference (architecture diagram, command explanations,
branching model, troubleshooting table), see the main [README](../README.md).

---

## 1. Project in one paragraph

A PySpark ETL pipeline that ingests claims CSV files from a Unity Catalog
landing volume, validates them (bad records go to a quarantine table with
reasons), deduplicates by claim ID, and idempotently MERGEs clean records into
a bronze Delta table. The code is defined as a **Declarative Automation Bundle
(DAB)** with two targets — `dev` and `prod` — and is deployed through the
Databricks bundle UI in dev. Production deployment will run through **GitHub
Actions** with a human approval gate, so Unity Catalog production data can
never be touched by accident.

---

## 2. Final folder structure

```
claims-processing-etl/
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD: validate+test on PR; deploy dev on
│                               #   merge; approval-gated prod deploy
├── data/
│   └── claims.csv              # 12-row sample: 8 valid, 1 duplicate,
│                               #   3 invalid (null amount / bad date / no id)
├── docs/
│   └── END_TO_END_PROJECT_GUIDE.md   # this file
├── resources/
│   └── claims_job.yml          # Job resource: serverless task, schedule,
│                               #   failure email, run-as, parameters
├── sql/
│   ├── setup_dev.sql           # UC setup for DEV (idempotent, IF NOT EXISTS)
│   └── setup_prod.sql          # UC setup for PROD (same, + locked-down
│                               #   service-principal grant templates)
├── src/
│   └── ingest_claims.py        # The PySpark ETL (read→validate→quarantine→
│                               #   dedup→transform→MERGE)
├── tests/
│   └── test_claims.py          # pytest unit tests for the transform logic
├── .gitignore                  # keeps .bundle/ state and caches out of Git
├── README.md                   # architecture, commands, ops runbook
├── databricks.yml              # Bundle root: variables + dev/prod targets
└── requirements-dev.txt        # test deps (pyspark, pytest) — job needs none
```

### Key design decisions baked into the structure

* **Volumes and tables are NOT bundle resources.** They are created once by
  the SQL scripts (`IF NOT EXISTS` only, no DROP/TRUNCATE/DELETE anywhere).
  A bundle deploy — or even a mistaken `bundle destroy` — can never touch
  landed data. This is the core protection for production data.
* **Everything environment-specific is a bundle variable** (`catalog`,
  `landing_path`, `job_pause_status`, …). The same code promotes dev → prod
  without edits. Dev: `claims_dev` catalog, job paused. Prod: `claims_prod`
  catalog, job runs daily 02:00 UTC.
* **One job definition serves both targets.** The deployed name is
  `[dev vsteja_de] claims_ingestion_dev` (dev mode namespaces it) and
  `claims_ingestion_prod` in prod.

---

## 3. What was built, phase by phase

### Phase 1 — Unity Catalog (DEV) setup ✅

Ran `sql/setup_dev.sql` in the SQL editor (Serverless Starter Warehouse):

| Object | Name | Notes |
| --- | --- | --- |
| Catalog | `claims_dev` | DEV catalog |
| Schema | `claims_dev.bronze` | Bronze layer |
| Volume | `claims_dev.bronze.landing` | Landing area for CSVs |
| Table | `claims_dev.bronze.claims_raw` | MERGE target for valid claims |
| Table | `claims_dev.bronze.claims_quarantine` | MERGE target for rejected rows |

Then uploaded `data/claims.csv` into the landing volume (12 rows total).

> The `users`-group GRANTs were commented out: trial/Free Edition workspaces
> have no built-in `users` group and the GRANTs would fail with
> `PRINCIPAL_DOES_NOT_EXIST`. Re-enable with a real group on a normal workspace.

### Phase 2 — Bundle authoring ✅

The full DAB was written and cleaned up:

* `databricks.yml`: bundle name, shared variables, `dev` target
  (`mode: development`, catalog `claims_dev`, schedule paused) and `prod`
  target (`mode: production`, catalog `claims_prod`, schedule unpaused).
* `resources/claims_job.yml`: serverless job (no cluster block — uses an
  `environment_key`/`environments` spec), failure email alert, max 1
  concurrent run, run-as the deploying user, parameters passed from variables.
* `src/ingest_claims.py`: pure-PySpark, no UDFs/pandas; small testable
  functions; logs a full processing summary; quarantine table untouched when
  there are no invalid rows.
* `tests/test_claims.py`: 7 unit tests against a local Spark session
  (schema contract, invalid-row reasons, hash fallback keys, dedup-latest,
  normalization, null-status default).
* Config fixes made during authoring: removed the `fail_on_unsafe_changes`
  flag (not needed on the direct deployment engine) and commented out the
  unsupported `users`-group permissions.

### Phase 3 — Validation ✅

`databricks bundle validate --target dev` → **Validation OK**
(bundle root: the Git folder; dev is the default target).

### Phase 4 — Git setup ✅

1. Created the private GitHub repo `claims-processing-etl` and linked a
   GitHub credential in Databricks (Git folder support).
2. Cloned it into a workspace Git folder:
   `/Repos/vsteja.de@gmail.com/claims-processing-etl`.
3. Created all 11 project files in the Git folder and committed.
4. **Push initially failed** — GitHub grants the Databricks integration
   access per-repository. Fixed by GitHub → Settings → Applications →
   Installed GitHub Apps → Databricks → **All repositories** → Save.
5. Pushed `main` successfully; contents verified on GitHub.

### Phase 5 — Dev deploy ✅

Deployed the dev target through the **bundle UI** (source-linked deployment —
deployed resources reference the source files in the working tree, so code
edits take effect on the next run without redeploying):

* Deployment version 1 created `jobs.claims_ingestion`.
* Deployed job: **`[dev vsteja_de] claims_ingestion_dev`** —
  job ID **667209733539054**, serverless compute, schedule paused (manual runs).

> Note: the Deploy button lives in the bundle editor (opened via
> `databricks.yml` in the Git folder). On some editor entry points it is not
> rendered — open the file from the folder's file tree.

### Phase 6 — Test runs and three serverless fixes ✅

The first run failed. Diagnosis (run output + step-by-step reproduction on a
cluster) found **three** independent issues, all fixed in
`src/ingest_claims.py` and pushed to GitHub (commit `5aaa1237`):

| # | Symptom | Root cause | Fix |
| --- | --- | --- | --- |
| 1 | `AnalysisException [UC_COMMAND_NOT_SUPPORTED.WITH_RECOMMENDATION]: input_file_name are not supported in Unity Catalog` | Serverless (Spark Connect + UC) blocks the legacy `input_file_name()` function | Use the built-in `_metadata` column: `F.col("_metadata.file_path")` |
| 2 | `DateTimeException [CANNOT_PARSE_TIMESTAMP]: Text '2026-13-99' could not be parsed` | In ANSI mode (serverless default), `to_date()` **raises** on unparseable input instead of returning NULL — these rows are meant to be quarantined, not crash the job | `F.try_to_date(col, fmt)` in all three places (`_valid_flags`, `deduplicate`, `transform`) |
| 3 | Run FAILED with error `SystemExit: 0` even though the pipeline had completed successfully | The serverless python-file runner executes scripts under IPython, where **any** `SystemExit` — even exit 0 — marks the workload FAILED | Success path ends cleanly (no `sys.exit`); only a failure exits nonzero: `if main() != 0: sys.exit(1)` |

### Phase 7 — Verified results ✅

Final verification run **539196836555665 — SUCCESS** (32 s):

* `claims_dev.bronze.claims_raw` → **8 rows**
  * duplicate `CLM-1002` deduplicated, keeping the later record (12500.00)
  * statuses uppercased (`submitted` → `SUBMITTED`), `claim_year` derived
* `claims_dev.bronze.claims_quarantine` → **3 rows**, reasons exactly as designed:
  `missing_or_invalid_claim_amount`, `invalid_claim_date`,
  `missing_or_empty_claim_id` (the last keyed by row-hash fallback)
* **Idempotency proven:** re-running the job kept 8/3 rows (MERGE updated
  existing rows, inserted none)

---

## 4. Current state snapshot

| Item | Value |
| --- | --- |
| Dev job | `[dev vsteja_de] claims_ingestion_dev` — job ID `667209733539054` |
| Last run | `539196836555665` — SUCCESS |
| Deployment | Bundle UI, dev target, source-linked (version 1) |
| Git folder | `/Repos/vsteja.de@gmail.com/claims-processing-etl` |
| Repo state | `main` up to date; fix commit `5aaa1237` |
| UC data | `claims_dev.bronze.{landing, claims_raw, claims_quarantine}` populated |
| Schedule | PAUSED in dev (manual runs only — intentional) |
| CI/CD | Workflow committed, **not yet active** (secrets not configured) |
| Prod | Not started (objects, deploy, SP) — checklist below |

---

## 5. Production checklist (the paused work)

Do these in order when resuming:

1. **Create a Databricks service principal** for CI
   (Admin settings → Identity & access → Service principals → Add, e.g.
   `claims-etl-deployer`) and generate an **OAuth secret** for it
   (Identity → OAuth secrets).
2. **Grant it UC rights**: run the uncommented-then-filled grant templates in
   `sql/setup_prod.sql` (replace `<deploy_sp_application_id>` etc. with the
   SP's application ID UUID).
3. **Add GitHub repo secrets** (Settings → Secrets and variables → Actions):
   `DATABRICKS_HOST` (`https://dbc-bba69e81-8b26.cloud.databricks.com`),
   `DATABRICKS_CLIENT_ID` (SP application ID),
   `DATABRICKS_CLIENT_SECRET` (OAuth secret).
4. **Create the `production` GitHub environment** (Settings → Environments →
   New environment → `production`) with **Required reviewers** — this is the
   human approval gate before every prod deploy.
5. **Protect `main`** (Settings → Branches): require pull requests + passing
   checks (the `validate-and-test` job).
6. **Run `sql/setup_prod.sql`** in this workspace (one-workspace fallback:
   separate `claims_prod` catalog) — or in the future prod workspace. It only
   creates objects that do not exist; existing data is untouched.
7. **Trigger the first prod deploy**: push to `main` (or re-run the workflow).
   CI will deploy dev → smoke-run it → pause at the `production` approval →
   deploy prod after a human approves.
8. **(Hardening)** Switch the prod job's run-as from the personal user to the
   service principal (`run_as.service_principal_name` in
   `resources/claims_job.yml`, via the `job_owner` variable), and point the
   `prod` target's `workspace.host` at the prod workspace if two workspaces
   are used.

### Decisions already made for prod

* Prod deploys only from GitHub Actions after approval — never manual
* `bundle destroy` is never run; volumes/tables live outside the bundle
* All prod SQL is `IF NOT EXISTS` — idempotent and non-destructive
* Rollback model: `git revert` the merge → CI redeploys last-known-good
  code; Delta time-travel (`RESTORE TABLE`) only for data incidents

---

## 6. How to operate (quick reference)

```bash
# Validate (run inside the Git folder; assistant chat can do this too)
databricks bundle validate --target dev

# Deploy dev (or use the bundle editor UI Deploy button)
databricks bundle deploy --target dev

# Trigger a run (or Jobs UI → Run now)
databricks bundle run --target dev claims_ingestion
```

Verify results in SQL:

```sql
SELECT count(*) FROM claims_dev.bronze.claims_raw;          -- 8
SELECT count(*) FROM claims_dev.bronze.claims_quarantine;   -- 3
SELECT invalid_reason, count(*) FROM claims_dev.bronze.claims_quarantine GROUP BY 1;
```

Change workflow: branch → edit → push → PR (CI validates + tests) → merge →
CI deploys dev → (later) approval → CI deploys prod.

---

## 7. Gotchas encountered (worth remembering)

* **GitHub integration access is per-repo** — new repos must be added to the
  Databricks GitHub App's allowed repositories (or use All repositories).
* **Serverless ANSI mode**: `to_date` throws on bad input; `try_to_date` is
  the safe parse. Anything that *can* receive bad landing data must tolerate it.
* **UC + Spark Connect**: `input_file_name()` unavailable; use `_metadata.file_path`.
* **Serverless python-file tasks run under IPython**: a raised `SystemExit`
  (even `exit 0`) fails the workload — end scripts cleanly on success.
* **Trial workspace quirks**: no built-in `users` group (GRANTs to it fail);
  the bundle editor's Deploy button is only visible when `databricks.yml` is
  opened from the folder file tree.
* **Source-linked deploys** read the working tree at run time — code edits
  apply on the next run with no redeploy, but a run started immediately after
  an edit may race the file sync (seen once; retry resolved it).
