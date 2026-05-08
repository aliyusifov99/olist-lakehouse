# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 8 — Pre-Commit Audit Checklist
# MAGIC
# MAGIC Documents the content-level scan to run before every `git push` from
# MAGIC the local repo. Complementary to `.gitignore` (which filters
# MAGIC filenames) and to GitHub's Secret Scanning (which fires *after* push,
# MAGIC as a backstop).
# MAGIC
# MAGIC Outputs:
# MAGIC - `audit_checklist.md` — human-readable checklist for the README
# MAGIC - `precommit_audit.sh` — automation of the manual scan steps
# MAGIC - `precommit_audit_examples.md` — annotated examples of true positives
# MAGIC   and benign false positives, so the checklist isn't ambiguous

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate the human-readable checklist

# COMMAND ----------

# This is the canonical text of the audit. The repo README references
# it directly. Keeping it in one place (this notebook) means updates
# land in one place, not three.

CHECKLIST_MD = """\
# Pre-Commit Audit Checklist

Run before every `git push`. Three checks plus one diff review. Total
time: ~30 seconds for a small commit. The cost of a missed leak (PAT
rotation, possible incident response, public-repo permanence) is
asymmetric, so even a 30-second check pays for itself many times over.

## 1. Scan staged files for credential prefixes

```bash
# Databricks PATs (start with `dapi`)
git diff --cached | grep -i 'dapi' && echo "❌ FAIL" || echo "✅ no dapi"

# GCP service account JSON markers
git diff --cached | grep -E 'private_key|private_key_id' \\
  && echo "❌ FAIL" || echo "✅ no GCP key markers"

# Generic high-entropy token patterns (GitHub, Slack, AWS)
git diff --cached | grep -E '(ghp_|gho_|ghs_|xoxb-|xoxp-|AKIA)[A-Za-z0-9]{16,}' \\
  && echo "❌ FAIL" || echo "✅ no third-party tokens"
```

If any of these print `❌ FAIL`, do **not** push. Open the diff,
identify the file and line, remove the value, and replace with a
secret-scope reference (`dbutils.secrets.get(...)`).

## 2. Scan the entire working tree for files that shouldn't exist

```bash
# .databrickscfg should NEVER be in the repo
find . -name '.databrickscfg' -not -path './.git/*' \\
  && echo "❌ FAIL" || echo "✅ no .databrickscfg"

# JSON files that aren't on the allow list (likely service account keys)
find . -name '*.json' -not -path './.git/*' \\
  -not -name 'package.json' -not -name 'package-lock.json' \\
  -not -name 'tsconfig.json' \\
  && echo "⚠️  REVIEW" || echo "✅ no unexpected JSON"
```

The `find` command for `.json` returns `⚠️  REVIEW` (not FAIL) because
some legitimate JSON files (dashboard exports, config) might appear.
Visually verify each is non-sensitive before pushing.

## 3. Verify databricks.yml has no inline token

```bash
# DABs config (Phase 9). MUST contain `host:`, MUST NOT contain `token:`.
grep -n '^token:' databricks.yml \\
  && echo "❌ FAIL — token in databricks.yml" \\
  || echo "✅ no inline token"
```

DABs authentication should resolve via `databricks auth login`'s
profile (`~/.databrickscfg`) or env vars, never via a literal token in
`databricks.yml`. If a token *is* there, someone added it for a quick
test and forgot to remove it. Reference:
https://docs.databricks.com/aws/en/dev-tools/bundles/authentication.html

## 4. Eyeball the diff one last time

```bash
git diff --cached
```

The grep checks are pattern-based — they don't catch a value that
doesn't match a known prefix (e.g., a custom API key with no prefix
convention). The eyeball pass catches everything else: hardcoded
URLs with embedded credentials, base64-encoded blobs that look out of
place, suspiciously long string literals.

If the diff is too big to review by eye, that's a signal to split the
commit, not to skip the review.

## What to do if a check fails

1. **Stop. Do not push.** Do not try to amend-and-force-push as a quick fix
   — if the value was ever pushed, force-pushing alone won't help.
2. **Rotate the credential first.** Even if the commit is still local,
   treat the value as compromised. Issue a new token/key, update the
   consumers, then revoke the old one in Databricks / GCP / GitHub.
3. **Then clean the working tree.** Remove the value, replace it with a
   `dbutils.secrets.get(scope, key)` reference (or move CLI auth to
   `~/.databrickscfg`), and re-stage.
4. **Re-run this audit.** The script must exit `0` before you push.
5. **If the credential was already pushed:** in addition to rotating,
   purge it from history with `git filter-repo` (or BFG), then
   force-push the rewritten history. Notify anyone who may have cloned
   the repo in the interim.
   Reference: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository
"""

import os
out_dir = f"/Workspace/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/phase_8"
os.makedirs(out_dir, exist_ok=True)

out_path = f"{out_dir}/audit_checklist.md"
with open(out_path, "w", encoding="utf-8") as f:
    f.write(CHECKLIST_MD)

print(f"✅ Wrote {out_path} ({len(CHECKLIST_MD)} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate the automation script

# COMMAND ----------

# A shell script that runs all three grep/find checks in sequence and
# returns a non-zero exit code if any FAIL. This makes it suitable
# for either manual execution (`./precommit_audit.sh`) or wiring into
# a real pre-commit hook later.
#
# Why bash and not Python: portable, zero install requirements, and
# `grep`/`find` are exactly the right tools for this job. A Python
# version would be ~5× longer with no functional improvement.

PRECOMMIT_SH = r"""#!/usr/bin/env bash
# precommit_audit.sh — content-level credential scan for olist-lakehouse
# Run from repo root before every `git push`.
#
# Exit code 0 = safe to push. Non-zero = fix and re-run.
#
# Generated by notebooks/phase_8_security_audit/08_pre_commit_audit.py.

set -uo pipefail
fail=0

echo "== Pre-commit audit =="

# ---- Check 1: staged-diff credential patterns ----
# We grep the *staged* diff specifically (`--cached`) because that's
# what's about to be committed. Unstaged changes can be inspected
# separately with a manual `git diff`.

if git diff --cached | grep -qi 'dapi'; then
  echo "❌ FAIL: Databricks PAT (dapi…) found in staged diff"
  fail=1
else
  echo "✅ no dapi tokens in staged diff"
fi

if git diff --cached | grep -qE 'private_key|private_key_id'; then
  echo "❌ FAIL: GCP service account key markers found in staged diff"
  fail=1
else
  echo "✅ no GCP key markers in staged diff"
fi

if git diff --cached | grep -qE '(ghp_|gho_|ghs_|xoxb-|xoxp-|AKIA)[A-Za-z0-9]{16,}'; then
  echo "❌ FAIL: third-party token (GitHub/Slack/AWS) found in staged diff"
  fail=1
else
  echo "✅ no third-party tokens in staged diff"
fi

# ---- Check 2: working-tree files that shouldn't exist ----

if find . -name '.databrickscfg' -not -path './.git/*' | grep -q .; then
  echo "❌ FAIL: .databrickscfg exists in working tree"
  fail=1
else
  echo "✅ no .databrickscfg in working tree"
fi

# JSON files outside the allow-list — REVIEW (warn, don't fail)
unexpected_json=$(find . -name '*.json' -not -path './.git/*' \
  -not -name 'package.json' -not -name 'package-lock.json' \
  -not -name 'tsconfig.json' 2>/dev/null)
if [ -n "$unexpected_json" ]; then
  echo "⚠️  REVIEW: unexpected JSON files (verify non-sensitive):"
  echo "$unexpected_json" | sed 's/^/      /'
else
  echo "✅ no unexpected JSON files"
fi

# ---- Check 3: databricks.yml inline token ----

if [ -f databricks.yml ]; then
  if grep -qE '^[[:space:]]*token:' databricks.yml; then
    echo "❌ FAIL: inline 'token:' in databricks.yml"
    fail=1
  else
    echo "✅ no inline token in databricks.yml"
  fi
else
  echo "ℹ️  databricks.yml not present (skipping that check)"
fi

# ---- Summary ----

echo ""
if [ "$fail" -eq 0 ]; then
  echo "✅ PASS — safe to push"
  echo "   Recommended: also run 'git diff --cached' for an eyeball review"
  exit 0
else
  echo "❌ FAIL — fix issues above before pushing"
  echo "   If a credential was already committed: see docs/audit_checklist.md"
  echo "   §'What to do if a check fails' — rotate first, then clean history."
  exit 1
fi
"""

out_path = f"{out_dir}/precommit_audit.sh"
with open(out_path, "w", encoding="utf-8") as f:
    f.write(PRECOMMIT_SH)

print(f"✅ Wrote {out_path} ({len(PRECOMMIT_SH)} chars)")
print(f"   Copy to repo root and `chmod +x precommit_audit.sh`.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate the examples doc (false positives + true positives)

# COMMAND ----------

# A reference doc that resolves the most common ambiguity:
# "the audit returned a hit, is it actually a problem?"
# Without this, an audit that flags a benign hit (e.g., the literal
# string 'dapi' inside a comment about Databricks PATs) trains you to
# ignore future hits — exactly the wrong response. Concrete examples
# tell you which hits are real and which to whitelist.

EXAMPLES_MD = """\
# Pre-Commit Audit — Examples

Resolves "is this hit a real leak or a false positive?"

## True positives (real leaks — must fix)

### Hardcoded PAT in a notebook cell

```python
# notebooks/phase_2/silver_orders.py — WRONG
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
  "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIE...\\n-----END...",
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
# notebooks/phase_8_security_audit/08_pre_commit_audit.py — SAFE
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

- Yes → true positive → rotate the credential first, then clean history (see [audit_checklist.md](audit_checklist.md#what-to-do-if-a-check-fails))
- No (just the prefix word in comments/docs) → false positive → push

If you can't tell at a glance, treat it as a true positive. The cost
of a false alarm is 30 seconds; the cost of a missed leak is hours.
"""

out_path = f"{out_dir}/precommit_audit_examples.md"
with open(out_path, "w", encoding="utf-8") as f:
    f.write(EXAMPLES_MD)

print(f"✅ Wrote {out_path} ({len(EXAMPLES_MD)} chars)")
print("\n=== Phase 8 artifacts written ===")
import os as _os
for fname in sorted(_os.listdir(out_dir)):
    fpath = f"{out_dir}/{fname}"
    print(f"   {_os.path.getsize(fpath):>6} bytes  {fname}")

# COMMAND ----------

