# Pre-Commit Audit — Examples

Resolves "is this hit a real leak or a false positive?"

## True positives (real leaks — must fix)

### Hardcoded PAT in a notebook cell

```python
# notebooks/phase_2_silver_transforms/silver_orders.py — WRONG
import requests
TOKEN = "random29042043982049"
requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"})
```

**Why it's a leak:** The token string is committed verbatim. Anyone
with read access to the repo can authenticate as you against the
workspace.

**Fix:**
```python
TOKEN = dbutils.secrets.get(scope="olist-scope", key="api-token")
```

### Service account JSON pasted into a markdown cell

```markdown
# Setup notes — WRONG

Here's the SA key for testing:
{
  "type": "service_account",
  "project_id": "olist-lakehouse-494119",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END...",
  ...
}
```

**Why it's a leak:** The full credential is in the file. Even if
it's "just for testing", testing keys have the same blast radius as
prod keys until rotated.

**Fix:** Delete the cell. Reference the GCS bucket via the storage
credential set up in Phase 0 — no JSON keys ever in code.

### Token in databricks.yml

```yaml
# databricks.yml — WRONG
workspace:
  host: https://my-workspace.gcp.databricks.com
  token: dapi3a4b5c...
```

**Why it's a leak:** YAML config commits to the repo by design.
Tokens go in `~/.databrickscfg` (which `.gitignore` excludes), not
here.

**Fix:** Remove the `token:` line. CLI auth resolves the token via
`databricks auth login`'s saved profile or env vars.

---

## False positives (benign — safe to push)

### Comment mentioning the prefix

```python
# notebooks/phase_8_security_audit/03_pre_commit_audit.py — SAFE
# The grep checks for 'dapi' because Databricks PATs start with that prefix.
```

**Why it's benign:** The literal word `dapi` appears in a comment, no
hex characters following. Grep flags it because the pattern is
case-insensitive prefix-only. You'll see this hit in the audit output
of *this very notebook*.

**Action:** None. Eyeball the hit, confirm it's a comment, push.

### Markdown describing token format

```markdown
Databricks PATs start with `dapi` followed by 32 hex characters.
```

**Why it's benign:** Documentation about the format, no actual token.

**Action:** None.

### `dapi` as a substring in unrelated identifiers

Not common in this project, but example: a column name `adapi_score`
would match `dapi`. The audit script uses `grep -i 'dapi'` rather
than a stricter regex like `dapi[0-9a-f]{32}` precisely because the
asymmetry favors over-flagging — a false positive costs you 5 seconds
of eyeballing; a false negative costs a credential rotation.

**Action:** Eyeball, confirm benign, push.

---

## Decision rule

When the audit flags a hit, ask: **"Is the actual credential value
present in the file?"**

- Yes → true positive → fix per RECOVERY_RUNBOOK.md
- No (just the prefix word in comments/docs) → false positive → push

If you can't tell at a glance, treat it as a true positive. The cost
of a false alarm is 30 seconds; the cost of a missed leak is hours.
