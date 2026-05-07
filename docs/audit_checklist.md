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
git diff --cached | grep -E 'private_key|private_key_id' \
  && echo "❌ FAIL" || echo "✅ no GCP key markers"

# Generic high-entropy token patterns (GitHub, Slack, AWS)
git diff --cached | grep -E '(ghp_|gho_|ghs_|xoxb-|xoxp-|AKIA)[A-Za-z0-9]{16,}' \
  && echo "❌ FAIL" || echo "✅ no third-party tokens"
```

If any of these print `❌ FAIL`, do **not** push. Open the diff,
identify the file and line, remove the value, and replace with a
secret-scope reference (`dbutils.secrets.get(...)`).

## 2. Scan the entire working tree for files that shouldn't exist

```bash
# .databrickscfg should NEVER be in the repo
find . -name '.databrickscfg' -not -path './.git/*' \
  && echo "❌ FAIL" || echo "✅ no .databrickscfg"

# JSON files that aren't on the allow list (likely service account keys)
find . -name '*.json' -not -path './.git/*' \
  -not -name 'package.json' -not -name 'package-lock.json' \
  -not -name 'tsconfig.json' \
  && echo "⚠️  REVIEW" || echo "✅ no unexpected JSON"
```

The `find` command for `.json` returns `⚠️  REVIEW` (not FAIL) because
some legitimate JSON files (dashboard exports, config) might appear.
Visually verify each is non-sensitive before pushing.

## 3. Verify databricks.yml has no inline token

```bash
# DABs config (Phase 9). MUST contain `host:`, MUST NOT contain `token:`.
grep -n '^token:' databricks.yml \
  && echo "❌ FAIL — token in databricks.yml" \
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

See `RECOVERY_RUNBOOK.md` (Subphase 8.3 — generated next). TL;DR:
**rotate the credential first**, *then* clean up the git history.
