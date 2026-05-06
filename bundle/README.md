# Databricks Asset Bundle — `olist-lakehouse`

This bundle defines the SDP pipeline and Unity Catalog schemas as
infrastructure-as-code. It is deliverable for the
`olist-lakehouse` portfolio project.

## What this bundle deploys

| Resource | Defined in | Type |
|---|---|---|
| `olist-medallion-pipeline` | `resources/olist_pipeline.yml` | Lakeflow Spark Declarative Pipeline (SDP) |
| `bronze` schema | `resources/olist_schemas.yml` | UC schema |
| `silver` schema | `resources/olist_schemas.yml` | UC schema |
| `gold` schema | `resources/olist_schemas.yml` | UC schema |
| `dlt_output` schema | `resources/olist_schemas.yml` | UC schema (SDP pipeline target) |

## What this bundle does NOT deploy

Deliberately out of scope for the portfolio version:

- **The catalog itself.** `olist_lakehouse_us` is the workspace's
  auto-created catalog. The bundle deploys schemas into
  it but does not own its lifecycle.
- **The SQL warehouse, dashboards, and alerts.** Phase 5 artifacts
  could be bundle-managed (`resources.dashboards`, `resources.sql_warehouses`),
  but were left as UI-managed for the portfolio. The pattern is
  identical to the schemas file.
- **Delta Sharing artifacts.** Phase 7's share + recipient could be
  bundle-managed similarly.
- **Jobs.** This project intentionally has no Lakeflow Jobs — SDP
  self-orchestrates the medallion (see project plan §18, "Why didn't
  you wrap the imperative chain in a Lakeflow Job?"). If a freshness-
  check job were added, it would land in `resources/olist_jobs.yml`.

## Targets

- **`dev`** (default): per-user paths under
  `/Workspace/Users/<email>/.bundle/...`, resources prefixed with
  `[dev <user>]`, schedules paused.
- **`prod`**: shared `/Shared/.bundle/prod/...` path, no prefix,
  group-level permissions enforced.

A deploy without `--target` goes to `dev` (because `default: true`).

## Deploy commands

```bash
# Validate without deploying — catches YAML errors and reference issues
databricks bundle validate --target dev

# Deploy to dev (default target)
databricks bundle deploy --target dev

# Run the deployed pipeline
databricks bundle run olist_medallion_pipeline --target dev

# Tear down what the bundle deployed
databricks bundle destroy --target dev
```

## Authentication

The bundle relies on the local Databricks CLI profile resolved via
`~/.databrickscfg`. No token lives in `databricks.yml`. The
`precommit_audit.sh` script explicitly checks that
`databricks.yml` does not contain a `token:` field.

## Status

The bundle YAML is **authored and validated**, but the actual
`databricks bundle deploy` was not executed against the workspace.
The SDP pipeline already exists from Phase 4 (created via the UI),
and a parallel bundle-deployed copy would either conflict or
duplicate it under a `[dev <user>]` prefix. The portfolio value of
DABs is the IaC definition itself — the YAML is reviewable, the
deploy command is documented, and a fresh-workspace replay would
work. This is documented honestly rather than concealing it.

## Extensions a production version would add

- **Variables block** for catalog name, workspace host, notification
  emails — parameterizes per-environment values.
- **`run_as`** to deploy under a service principal in prod, isolating
  who-deployed from who-runs.
- **Notifications** (`email_recipients` + `alerts`) on the pipeline
  resource for failure alerting.
- **CI/CD** — `databricks bundle deploy` from GitHub Actions with
  M2M OAuth to a service principal. The bundle YAML doesn't change;
  only the deployer changes.