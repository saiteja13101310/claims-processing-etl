# Claims Processing ETL — Dev → Prod Process Guide

**Purpose:** explains, step by step, how this project moves from development to production, so that anyone — even someone new to the project — can understand what happens, who does what, and what protects production data.

**Companion docs:**

* `docs/END_TO_END_PROJECT_GUIDE.md` — the full story of how the project was built (history, bugs fixed, current state)
* `README.md` — architecture, command reference, troubleshooting runbook

---

## 1. The big picture

This project runs in **one Databricks workspace** with **two Unity Catalog catalogs** that act as the two environments. There is ONE codebase (a Declarative Automation Bundle) that is pointed at different catalogs by variables — dev code and prod code are literally the same files.

```
                      one Databricks workspace
 ┌────────────────────────────────────────────────────────────────────┐
 │                                                                    │
 │   DEV environment                     PROD environment            │
 │   catalog: claims_dev                 catalog: claims_prod         │
 │   ├─ bronze.landing        (volume)    ├─ bronze.landing (volume)  │
 │   ├─ bronze.claims_raw     (table)     ├─ bronze.claims_raw        │
 │   ├─ bronze.claims_quarantine          ├─ bronze.claims_quarantine │
 │   └─ bronze.claims_run_audit           └─ bronze.claims_run_audit  │
 │                                                                    │
 │   job: claims_ingestion_dev           job: claims_ingestion_prod  │
 │        (schedule PAUSED, manual)           (daily 02:00 UTC)        │
 └────────────────────────────────────────────────────────────────────┘

   Code changes travel the ONLY road to prod:

   developer → branch → pull request → CI tests → merge to main
      → CI deploys DEV → CI smoke-runs the dev job
      → CI PAUSES at the production approval gate
      → a human clicks Approve → CI deploys PROD
```

The key idea: **nobody ever deploys to prod by hand**. Production changes arrive exclusively through GitHub Actions after a human approval.

---

## 2. Dev vs prod at a glance

| | DEV | PROD |
| --- | --- | --- |
| Catalog | `claims_dev` | `claims_prod` |
| Job name | `[dev …] claims_ingestion_dev` | `claims_ingestion_prod` |
| Job schedule | PAUSED — runs only when someone clicks Run | ACTIVE — runs daily at 02:00 UTC |
| Landing volume | `claims_dev.bronze.landing` | `claims_prod.bronze.landing` |
| Data in tables | test samples (15 valid / 5 quarantined) | real claims data from producers |
| Who can write tables | the developer (owner) | ONLY the pipeline's run-as service principal |
| How changes arrive | bundle deploy of the `dev` target (CI or UI) | CI deploy of the `prod` target, after approval |
| Audit trail | `claims_dev.bronze.claims_run_audit` | `claims_prod.bronze.claims_run_audit` |

---

## 3. The four rules that keep production safe

1. **Promote CODE, never DATA.** Creating `claims_prod` copies nothing from dev. Test rows do not leak into production. The pipeline reads whatever files are in the prod landing volume — which starts empty until real producers deliver.
2. **The only road to prod is CI.** Merges to `main` trigger the workflow: deploy dev → smoke-test → stop at the approval gate → deploy prod. A manual `bundle deploy --target prod` is the anti-pattern this design exists to prevent.
3. **Data objects live outside the bundle.** The landing volume and all tables are created by `sql/setup_dev.sql` / `sql/setup_prod.sql` using `IF NOT EXISTS` only — no DROP, TRUNCATE, or DELETE anywhere. Even a mistaken `bundle destroy` cannot touch landed data.
4. **Least privilege.** In prod, the run-as service principal gets exactly: READ on the landing volume + MODIFY on the three tables. Humans get read-only. Nobody but the pipeline writes to bronze.

---

## 4. One-time production setup (do once, in this order)

### Step 1 — Create the CI service principal
Databricks workspace → Admin settings → Identity & access → Service principals → Add → name it `claims-etl-deployer`. Then in the SP's page → OAuth secrets → Generate a secret. This machine identity is what GitHub Actions will authenticate as — never your personal token.

### Step 2 — Create the prod catalog objects and grants
Run `sql/setup_prod.sql` in a SQL editor. It creates the `claims_prod` catalog, `bronze` schema, `landing` volume, and the three tables. Before running, uncomment the grant templates at the bottom and replace `<deploy_sp_application_id>` / `<run_as_sp_application_id>` with the SP's application ID (a UUID). Re-running the script is always safe — it only creates what does not exist.

### Step 3 — Add GitHub Actions secrets
GitHub repo → Settings → Secrets and variables → Actions → New repository secret, three times:

* `DATABRICKS_HOST` = `https://dbc-bba69e81-8b26.cloud.databricks.com`
* `DATABRICKS_CLIENT_ID` = the service principal's application ID
* `DATABRICKS_CLIENT_SECRET` = the OAuth secret from Step 1

### Step 4 — Create the `production` environment with an approval gate
GitHub repo → Settings → Environments → New environment → name `production` → add **Required reviewers**. Every prod deploy now pauses until one of those people clicks Approve. This is the human control point of the whole design.

### Step 5 — Protect `main`
GitHub repo → Settings → Branches → Branch protection for `main`: require pull requests before merging + require the `validate-and-test` workflow to pass. This guarantees nothing reaches prod that was not reviewed and tested.

### Step 6 — First prod deploy
Push any commit to `main` (or manually re-run the workflow). The pipeline will: deploy dev → run the dev job as a smoke test → stop for approval → after approval, deploy prod and create `claims_ingestion_prod` with its schedule ACTIVE. Verify with the queries in section 7.

---

## 5. The everyday flow: shipping a change to prod

Say you fixed a bug or added a column to the transform. The path from your keyboard to prod:

1. **Create a branch** off `main` (never commit directly to `main`).
2. **Make the change** in the Git folder; run `pytest` where practical for transform logic.
3. **Open a pull request.** CI immediately runs `databricks bundle validate` + the unit test suite. A red ✗ blocks everything.
4. **A teammate reviews, then the PR merges.** (Branch protection requires this.)
5. **CI deploys the DEV target automatically.** The dev job now runs the new code (dev deploys are source-linked — code edits apply on the next run).
6. **CI smoke-runs the dev job** and checks it goes green. If the smoke test fails, the pipeline stops — prod is never touched.
7. **CI pauses at the `production` approval gate.** Reviewers get a GitHub notification.
8. **A human clicks Approve.**
9. **CI deploys the PROD target.** Job `claims_ingestion_prod` now runs the new code on its next 02:00 UTC schedule (or can be run once manually for verification).
10. **Verify** using the queries in section 7.

Every stage exists to answer one question before prod is touched: *did it work in dev?* If the answer is no, prod is never reached.

---

## 6. What the first prod deploy actually creates

* **Job `claims_ingestion_prod`** — the same `resources/claims_job.yml` definition, pointed at `claims_prod` via bundle variables. Its schedule is **ACTIVE from day one** (daily 02:00 UTC), unlike dev which is intentionally paused.
* **No data.** `claims_prod.bronze.claims_raw` starts empty and fills only as producers upload real claims files to `/Volumes/claims_prod/bronze/landing/`.
* **Full audit from run one.** Every prod run (success or failure) appends to `claims_prod.bronze.claims_run_audit` with all counts, the MERGE inserted/updated numbers, and the error message if anything failed.

---

## 7. Verifying prod after a deploy

Run in any SQL editor:

```sql
-- Latest pipeline runs and their outcomes
SELECT run_id, started_at, rows_read, valid_rows, invalid_rows,
       raw_rows_inserted, raw_rows_updated, status, error_message
FROM claims_prod.bronze.claims_run_audit
ORDER BY started_at DESC;

-- Current contents
SELECT count(*) FROM claims_prod.bronze.claims_raw;
SELECT count(*) FROM claims_prod.bronze.claims_quarantine;
SELECT invalid_reason, count(*)
FROM claims_prod.bronze.claims_quarantine GROUP BY 1;
```

Also check Jobs → `claims_ingestion_prod` for run history, or:

```sql
SELECT run_id, result_state, period_start_time
FROM system.lakeflow.job_run_timeline
WHERE job_name = 'claims_ingestion_prod'
ORDER BY period_start_time DESC LIMIT 10;
```

---

## 8. Rollback and emergencies

* **Bad code shipped to prod:** `git revert` the merge commit and merge the revert. CI redeploys the last-known-good code through the same approval flow. This is the standard, preferred rollback.
* **Bad data landed in prod:** Delta time travel lets you inspect or restore any earlier table version (`RESTORE TABLE … TO VERSION`). Treat this as a last resort — always investigate the run audit table first to understand exactly which run wrote what.
* **Hotfix path:** same PR flow, marked urgent. The approval gate is NOT bypassed even for hotfixes — that is the point of having it.

---

## 9. FAQ

**Does creating prod copy my dev data over?**
No — deliberately. Dev has test rows; prod starts empty and only ever contains real files uploaded to its own landing volume.

**Can I deploy to prod by hand just this once?**
Technically a workspace admin could; the design considers it a never-event. If you ever feel the need, that is the signal to fix the CI flow instead.

**Can I trigger the prod job manually?**
Yes, from the Jobs UI — manual *runs* are a normal verification tool. Manual *deploys* are the anti-pattern.

**What if the prod landing volume is empty?**
The job logs "no files found", writes a SUCCESS audit row with `rows_read = 0`, and exits cleanly. Not an error.

**Who can approve prod deploys?**
Exactly the people listed as Required reviewers on the GitHub `production` environment. Keep this list small and accountable.

**What happens to dev once prod exists?**
Nothing changes. Dev remains the safe place to experiment; every merge to `main` keeps deploying dev first, then prod. One codebase, two environments, forever.

---

## 10. Quick reference

* Bundle root (Git folder): `/Repos/vsteja.de@gmail.com/claims-processing-etl`
* Validate: `databricks bundle validate --target dev`
* Deploy dev: `databricks bundle deploy --target dev` (CI does this automatically on merge)
* Run dev job: Jobs UI → `claims_ingestion_dev` → Run now
* CI workflow: `.github/workflows/deploy.yml`
* Prod setup SQL: `sql/setup_prod.sql` (idempotent, `IF NOT EXISTS` only)
