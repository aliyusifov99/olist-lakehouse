# Security Review Report

Date: 2026-05-07

Scope: local repository scan of tracked project files plus ignored/untracked file review. Focus areas were exposed API tokens, Databricks credentials, cloud keys, private keys, environment files, hard-coded identities, and high-risk Databricks governance patterns.

## Executive Summary

No critical exposed credential was found in the tracked repository files.

The scan did not identify live-looking Databricks PATs, GitHub tokens, AWS keys, GCP service account JSON keys, private keys, `.env` files, JDBC passwords, bearer tokens, or inline API keys.

The main findings were lower severity and have been remediated in the current working tree:

1. A personal email address was hard-coded in a governance notebook.
2. A secret-scope demonstration notebook printed secret-derived values and used placeholder text in a way that could become unsafe if copied with a real secret.
3. The Databricks workspace host was committed. This was not a credential, but it revealed workspace metadata.
4. The repo has good ignore rules, but ignored local notes and OS metadata are present on disk and should remain untracked.

## Findings

### 1. Hard-coded personal identity in access-control SQL

Severity: Medium

Status: Remediated

File: `notebooks/phase_6_governance/06_governance_access_control.py`

Original issue:

The notebook used a literal personal email address in the self-revoke / self-grant example.

Risk:

The email address is not an API secret, but it is personal information and ties the project to a specific account. In public repos, portfolio projects, screenshots, or demos, this exposes identity metadata and makes the notebook less reusable.

Recommendation:

The literal user has been replaced with a documentation-only placeholder:

```sql
-- Example only:
-- REVOKE SELECT ON TABLE olist_lakehouse_us.bronze.geolocation FROM `<YOUR-EMAIL>`;
-- GRANT SELECT ON TABLE olist_lakehouse_us.bronze.geolocation TO `<YOUR-EMAIL>`;
```

For a production pattern, prefer group-based access:

```sql
GRANT SELECT ON TABLE olist_lakehouse_us.bronze.geolocation TO `data_engineers`;
```

### 2. Secret demo can become unsafe if copied with real secrets

Severity: Medium

Status: Remediated

File: `notebooks/phase_8_security_audit/08_create_secret_scope.py`

Lines:
- `52-56`: writes placeholder secret value with `w.secrets.put_secret(...)`
- `104`: reads a secret using placeholder key text
- `108`: prints the returned secret value
- `112-113`: constructs and prints a path derived from the secret value
- `117`: prints the secret length

Risk:

The current committed value is a placeholder, not a real credential. However, this notebook trains a dangerous habit: reading a secret and printing it or derived values. Databricks usually redacts direct secret values in notebook output, but redaction should not be relied on as the primary security control. Derived strings, partial values, lengths, URLs, or paths can still leak useful information.

Recommendation:

The secret-scope creation demo now avoids printing secret material or derived values. It verifies only that retrieval returned a value:

```python
bucket_name = dbutils.secrets.get(scope="olist-scope", key="gcs-bucket-name")
assert bucket_name, "Secret value should be present"
print("Secret retrieval succeeded")
```

The placeholder key mismatch was also fixed. The notebook stores and retrieves key `gcs-bucket-name`.

### 3. Databricks workspace URL is committed

Severity: Low

Status: Remediated

File: `databricks.yml`

Original issue:

The file committed a concrete Databricks workspace URL. Exported dashboard PDFs also contained embedded Databricks navigation links with the workspace identifier.

Risk:

The host URL is not a token and does not grant access. It does reveal the workspace identifier and cloud/provider region context. For a private project this is usually acceptable. For a public portfolio repo, it is better to avoid publishing workspace-specific metadata unless intentionally shared.

Recommendation:

The host is now supplied through a Databricks bundle variable:

```yaml
workspace:
  host: ${var.databricks_host}
```

Use `--var databricks_host=https://<workspace-host>` or `BUNDLE_VAR_databricks_host` locally. Continue ensuring no `token:` field appears in `databricks.yml`.

The embedded workspace IDs in the dashboard PDF links were also replaced with same-length placeholder IDs so the files no longer expose the original workspace metadata.

### 4. Ignored local files and notes exist on disk

Severity: Low

Files observed as ignored/untracked:

- `.DS_Store`
- `notebooks/.DS_Store`
- `phases_notes_and_plan/*.md`
- `phases_notes_and_plan/.DS_Store`

Risk:

These files are currently ignored and not tracked, which is good. Local notes sometimes contain setup details, project IDs, account names, or operational notes that may not belong in a public repository.

Recommendation:

Leave them untracked unless deliberately sanitized. Before changing `.gitignore`, re-check these files for sensitive setup details.

## Positive Controls Observed

- `.gitignore` excludes Databricks auth/config artifacts: `.databricks/`, `.databrickscfg`, `.bundle/`, `.databricks-cli/`.
- `.gitignore` excludes `.env`, `*.env`, Python virtualenvs, caches, raw data files, logs, and GCP JSON keys.
- `databricks.yml` uses a host variable and contains no inline `token:` value.
- Phase 8 includes a pre-commit audit script and checklist for common credential patterns.
- Notes indicate GitHub/Databricks auth uses OAuth rather than a committed PAT.
- Raw CSV/parquet data is excluded from the repository.

## Scan Commands Used

Representative local checks:

```bash
rg -n -i "(api[_-]?key|secret|token|password|client[_-]?secret|private[_-]?key|access[_-]?key|databricks[_-]?token|bearer|authorization|jdbc|account[_-]?key|sas[_-]?token|BEGIN (RSA|OPENSSH|PRIVATE) KEY)" .
find . -maxdepth 4 -type f \( -name ".env*" -o -name "*secret*" -o -name "*credential*" -o -name "*.pem" -o -name "*.key" -o -name "*token*" \) -print
rg -n "dapi[a-z0-9]+|ghp_[A-Za-z0-9_]+|xox[baprs]-|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{35}" .
git ls-files --ignored --exclude-standard -o
```

No dedicated secret-scanning binary such as `gitleaks`, `trufflehog`, or `detect-secrets` was available in this local environment, so the review used repository-aware `rg`, `find`, and `git` checks.

## Recommended Next Steps

1. Run a dedicated scanner before pushing public changes:

```bash
gitleaks detect --source . --verbose
```

2. If any real credential is ever found in git history, rotate it first, then clean history.
