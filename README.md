# Claims Processing ETL Pipeline (Declarative Automation Bundle)

A production-ready Databricks project that ingests claims CSV files from a
Unity Catalog volume into a bronze Delta table — built and tested in **Dev**,
safely promoted to **Prod** through Git branches, pull requests, and CI/CD.

Designed for two topologies:

1. **Two workspaces** (Dev + Prod): set `workspace.host` per target in `databricks.yml`.
2. **One workspace fallback** (Databricks Free Edition / single workspace — the
   default here): both targets deploy to the same workspace, separated by
   catalog (`claims_dev` vs `claims_prod`). Dev mode namespaces job names so
   dev and prod never collide.

**You never need the Databricks CLI on your own machine.** Bundle commands run
inside GitHub Actions, and you can also ask the Databricks assistant chat to
run them for you in the workspace.

---

## 1. Architecture

```
                     GitHub repo (single source of truth)
                        main  <--  pull requests  <--  feature/*
                                      |                    |
                                      | CI: validate +    | (PR checks:
                                      |     unit tests    |  same, blocking)
                                      v
                      ┌──────── DEV ────────┐   ┌──────── PROD ───────┐
                      │ catalog: claims_dev  │   │ catalog: claims_prod│
                      │ schema: bronze       │   │ schema: bronze      │
                      │ volume: landing      │   │ volume: landing     │
                      │                      │   │                     │
                      │ claims_ingestion job │   │ claims_ingestion job│
                      │ (dev mode, paused)   │   │ (prod mode, daily)  │
                      └──────────────────────┘   └─────────────────────┘

Landing volume (*.csv)
   │  read as strings
   ▼
validate contract (required columns)
   │
   ├─ invalid rows ──► claims_quarantine (reason + raw values, idempotent MERGE)
   ▼
deduplicate by claim_id (latest claim_date wins)
   │
   ▼
transform (trim/upper, cast date/decimal, derive claim_year)
   │
   ▼
Delta MERGE into claims_raw on claim_id  ── idempotent: re-run safe
```

## 2. Project files and their purpose

| File | Purpose |
| --- | --- |
| `databricks.yml` | Bundle entry point: name, shared variables, `dev`/`prod` targets (mode, catalog, paths) |
| `resources/claims_job.yml` | The `claims_ingestion` job resource: serverless compute, schedule, alerts, run-as, permissions |
| `src/ingest_claims.py` | The PySpark ETL: read → validate → quarantine → dedup → transform → MERGE, with logging |
| `sql/setup_dev.sql` | Idempotent UC setup for dev: catalog, schema, `landing` volume, tables, grants |
| `sql/setup_prod.sql` | Same for prod; non-destructive by design, locked-down grants template |
| `tests/test_claims.py` | Pytest unit tests for the transform functions (run in CI on every PR) |
| `data/claims.csv` | 12-record sample file: 8 valid, 1 duplicate, 3 invalid (null amount, bad date, missing id) |
| `.github/workflows/deploy.yml` | CI/CD: validate + test on PR, deploy dev on merge, approval-gated prod deploy |
| `requirements-dev.txt` | Test/dev dependencies (pyspark + pytest) — the job itself needs none |
| `.gitignore` | Keeps `.bundle/` state, caches, and venvs out of Git |

> Volumes and tables are deliberately **outside** the bundle: `databricks.yml`
> contains no `volumes:` or `schemas:` resources, so no bundle deploy or
> `destroy` can ever touch production storage. They are created once by the
> SQL scripts.

## 3. Prerequisites

* A Databricks workspace with Unity Catalog (metastore attached).
* A Git hosting account (GitHub is used here) for the branching workflow.
* Admin rights on the dev workspace (to create catalogs).
* Nothing installed locally — no CLI, no Python.

## 4. Setup step by step

### Step 1 — Create the Unity Catalog objects

1. Sidebar → **Edit > Queries** → new query.
2. Warehouse: start and select **Serverless Starter Warehouse**.
3. Paste the contents of `sql/setup_dev.sql` → **Run**.
4. Expected output: statements succeed in order; `claims_dev` appears in
   Catalog Explorer with schema `bronze`, volume `landing`, and tables
   `claims_raw` + `claims_quarantine`.
5. Before the first prod deployment, repeat in the PROD workspace with
   `sql/setup_prod.sql` (or, in the one-workspace fallback, run it in this
   workspace — it only creates objects that do not exist yet).

> The scripts are safe to re-run: everything is `IF NOT EXISTS`, and there
> are no DROP/TRUNCATE/DELETE/UPDATE statements anywhere.

### Step 2 — Upload the sample data (Dev)

1. Catalog Explorer → `claims_dev` → `bronze` → volume **landing**.
2. Click **Upload to this volume** and add `data/claims.csv`.
3. Expected result: the file is listed at
   `/Volumes/claims_dev/bronze/landing/claims.csv`.

### Step 3 — Get the project into Git

1. Create an empty GitHub repo, e.g. `claims-processing-etl`.
2. Push this project to it. Easiest from the workspace: download the folder
   (file tree → download) and upload through GitHub's web UI, or clone into
   a workspace Git folder (sidebar → **Git**) and commit + push from there.
3. Protect `main`: repo Settings → Branches → add a rule requiring pull
   requests before merging and passing status checks.

### Step 4 — Wire up CI/CD secrets and approvals

1. Create a Databricks **service principal** (Admin settings → Identity &
   access → Service principals) and generate an OAuth secret for it.
2. Grant it the dev + prod UC rights from the SQL scripts (replace the
   placeholders in `sql/setup_prod.sql` with its application ID).
3. GitHub repo → Settings → Secrets and variables → Actions → add
   `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`.
4. Settings → Environments → create `production` → **Required reviewers**
   (this is the human approval gate for prod deploys).

## 5. The bundle commands (and what they do)

These run inside GitHub Actions automatically. If you ever want them run
interactively, ask the Databricks assistant chat — no local install needed.

```bash
databricks bundle validate -t dev
```
Checks `databricks.yml` + all included resource files against the workspace
(resolving variables like `${var.catalog}`), confirms the job definition is
well-formed, and that referenced files (`../src/ingest_claims.py`) exist.
Nothing is deployed; nothing changes in the workspace.

```bash
databricks bundle deploy -t dev
```
Uploads `src/ingest_claims.py` to the bundle's workspace folder and creates
or updates the dev job (named with your user prefix in dev mode, e.g.
`[vsteja.de@gmail.com] claims_ingestion_dev [dev]`). Re-running is cheap —
files sync with `--force` in development mode.

```bash
databricks bundle run -t dev claims_ingestion
```
Triggers the deployed job and streams the run. Expected log output:

```
Landing summary for /Volumes/claims_dev/bronze/landing/*.csv
  rows read           : 12
  valid rows          : 8
  duplicates dropped  : 1 (latest claim_date kept)
  invalid rows        : 3 (quarantined)
MERGE into claims_dev.bronze.claims_raw -> inserted=8 updated=0
MERGE into claims_dev.bronze.claims_quarantine -> inserted=3 updated=0
Claims ingestion completed successfully.
```

```bash
databricks bundle validate -t prod
```
Same validation, but resolves the prod target: `claims_prod` catalog, prod
landing path, unpaused schedule. Production mode also enforces stricter
rules — e.g. the deploy must not create resources outside the bundle root.

```bash
databricks bundle plan -t prod    # CLI 0.2xx+; if unavailable, validate + the deploy log cover it
```
Prints the **deployment plan** — exactly which resources will be created,
changed, or (only if you removed them from the bundle) deleted — without
applying anything. This is your pre-deploy review artifact.

```bash
databricks bundle deploy -t prod
```
Applies the plan to the prod workspace/target. The CLI asks for confirmation
interactively; CI passes `--auto-approve` because the human approval already
happened at the GitHub `production` environment gate. First deploy creates
`claims_ingestion_prod`; later deploys update in place — files and tables are
versioned, never mass-deleted.

> **Never** run `databricks bundle destroy -t prod` in this project's prod:
> it removes bundle-deployed resources. Even then it cannot touch your
> volumes/tables (they are outside the bundle) — but avoid it on principle.

## 6. Verifying results in SQL

```sql
SELECT count(*) AS total_claims FROM claims_dev.bronze.claims_raw;          -- 8
SELECT claim_id, status, claim_amount, claim_year FROM claims_dev.bronze.claims_raw;
SELECT invalid_reason, count(*) FROM claims_dev.bronze.claims_quarantine GROUP BY 1;
```

Expected: `CLM-1002` appears once with amount **12500.00** (the later of the
duplicates won), statuses are uppercase, quarantine holds 3 rows with
reasons `invalid_claim_date`, `missing_or_invalid_claim_amount`,
`missing_or_empty_claim_id`.

## 7. Git branching and promotion

The golden path:

```
feature/<name> ──► push ──► PR checks (validate + tests)
                             │ you fix / iterate
                             ▼
                       dev deploy (CI, on merge of PR branch to main via
                       your own PR into main, or push directly per your policy)
                             ▼
                       human approval (GitHub `production` environment)
                             ▼
                       prod deploy (CI)
```

### Create a feature branch and make a change

1. Open the repo in the workspace (**Git** sidebar) or on GitHub.
2. Create the branch: **Git → Branches → New** `feature/quarantine-alerting`
   (on GitHub: Branch dropdown → type the new name).
3. Edit e.g. `src/ingest_claims.py`, save, then **Git → Commit & Push**
   (or commit through a PR on GitHub if you edited in its web editor).

### Open a pull request

GitHub → Pull requests → New: base `main`, compare `feature/...`. The
workflow runs: unit tests + `bundle validate` for **both** targets. Merge
only when green. Squash-merge keeps history clean.

### Promote to prod

On merge, CI deploys dev, smoke-runs the job, then pauses at the
`production` environment gate. An approver clicks **Approve and deploy**;
the workflow validates the prod target, prints the deploy plan, and applies
it. Nothing about prod data changes — only job code/config.

### Roll back safely (never touching prod data)

* **Code/config rollback:** on GitHub, `git revert` the bad merge commit
  (or click *Revert* on the merged PR) → merge the revert PR → CI redeploys
  the last-known-good bundle to prod. Deployment state files make this an
  update, not a rebuild.
* **Data check:** because the pipeline MERGEs idempotently, re-running the
  fixed job corrects rows — it never duplicates them.
* **Emergency stop:** pause the job (Jobs UI → Schedule → Pause) —
  the landing volume keeps every raw file; nothing is lost.
* **Data rollback (rare, manual):** use `DESCRIBE HISTORY` on the Delta table
  and `RESTORE TABLE ... TO VERSION AS OF <n>` — deliberate, explicit, and
  only with stakeholder approval.

## 8. Modifying the dev job and promoting (worked example)

Goal: reject claims over 250,000 as suspicious and quarantine them.

1. Branch `feature/amount-ceiling`.
2. In `src/ingest_claims.py` → `_valid_flags`, add
   `amount_ceiling_ok = F.col("claim_amount").cast("DECIMAL(12,2)") <= 250000`
   and include `~amount_ceiling_ok → "amount_over_ceiling"` in the reason
   chain (both in `split_valid_invalid`).
3. Add a test in `tests/test_claims.py` asserting an oversized row is
   quarantined with that reason.
4. PR → CI green → merge → dev deploys automatically → smoke run passes
   → approve at the prod gate → prod updates.
5. Emergency rollback: revert the PR merge.

## 9. Service principal ownership (production hardening)

* **Deploy SP** authenticates CI (`DATABRICKS_CLIENT_ID/SECRET`) and owns the
  bundle's deployed resources. On Azure, prefer OIDC federation instead of a
  secret (see the commented block at the bottom of the workflow).
* **Run-as SP** is the job's `run_as` identity in prod (change `job_owner` and
  the `run_as` block in `resources/claims_job.yml` from `user_name` to
  `service_principal_name`). It gets `READ VOLUME` on landing + `MODIFY/SELECT`
  on the bronze tables — nothing else.
* Humans get read-only grants. Personal accounts never own prod resources.

## 10. Unity Catalog permissions explained

| Permission | Grants | Typical holder |
| --- | --- | --- |
| `USE CATALOG` / `USE SCHEMA` | visibility of the catalog/schema | everyone who queries |
| `READ VOLUME` | read/list files in a volume | the job's run-as SP |
| `WRITE VOLUME` | upload/delete files in a volume | data producers only |
| `SELECT` | read table rows | analysts, the job SP (needed for MERGE) |
| `MODIFY` | insert/update/delete rows | the job's run-as SP only |
| `CREATE TABLE` | create tables in a schema | deploy SP (first deploy, if setup SQL not run) |
| Owner | full control incl. DROP | deploy SP (prod), you (dev) |

Note `MERGE` needs **SELECT + MODIFY** on the target table, plus
`READ VOLUME` on the source volume — the most common cause of a first-run
permission failure.

## 11. Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `Path not found` on run | No CSV uploaded, or `--landing-path` glob wrong | Upload file; check `/Volumes/<cat>/bronze/landing/*.csv` |
| `TABLE_OR_VIEW_NOT_FOUND` on MERGE | Setup SQL never ran for that target | Run `sql/setup_dev.sql` / `setup_prod.sql` |
| `PERMISSION_DENIED` / `UC not authorized` | run-as lacks `SELECT+MODIFY` on the table or `READ VOLUME` | Grant per section 10 |
| Deploy error: `spark_python_task` + no cluster | Task references no `environment_key` | Keep `environment_key: default` + `environments:` block (serverless) |
| `bundle validate` fails on `${var...}` | Typo in variable name, or used in a target where it has no default | Check spelling; ensure a default or per-target override exists |
| Prod deploy says `production mode requires Git` | Deploying prod target from a non-Git folder | Deploy from the repo clone (Git folder) or via CI — never a loose copy |
| Duplicate rows in target after rerun | MERGE key not matched (e.g. whitespace in claim_id) | Pipeline trims keys already; check quarantine table for clues |
| Job says 0 rows read but files exist | Volume is empty of `*.csv` or wrong target's volume | Check the job's parameters reflect the right `${var.landing_path}` |

## 12. Deployment verification checklist

- [ ] `bundle validate -t dev` and `-t prod` exit 0
- [ ] Unit tests pass in CI on the PR
- [ ] Dev deploy succeeded; smoke run shows the expected summary (8/1/3)
- [ ] `claims_raw` rows: correct count, no duplicate `claim_id`, statuses uppercase
- [ ] `claims_quarantine`: 3 rows with correct reasons
- [ ] Re-run the job: `inserted=0 updated=8` (idempotency proven) *
- [ ] Prod plan reviewed (workflow log / `bundle plan`) — **no unexpected deletions**
- [ ] Prod approval gate configured and used
- [ ] Prod job run green; tables in `claims_prod` populated
- [ ] Failure email delivered to `alert_email` (test by a deliberate bad file in dev)
- [ ] Rollback rehearsed: revert PR merged and prod redeployed

\* `updated=8` because `_ingested_at` changes every run — rows re-match and
update, proving MERGE idempotency.

## 13. Production readiness summary

* Dev/prod separated by target + catalog (+ workspace host when available)
* Service principal auth for CI; run-as SP (or delegated user) for the job
* All environment paths via bundle variables — zero hardcoded values
* Email alert on job failure; daily schedule in prod, paused in dev
* Data quality: contract validation, quarantine with reasons, unit tests
* Volume retention: volumes/table outside the bundle; nothing in CI or DAB
ever deletes them; all setup SQL idempotent
* Rollback: git revert → redeploy; Delta time-travel for data incidents
