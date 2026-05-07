# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 8.1 — Create Secret Scope (Demonstration)
# MAGIC
# MAGIC Creates `olist-scope` (Databricks-backed) and stores one demonstration
# MAGIC secret (`gcs-bucket-name`). The bucket name is **not** sensitive — this
# MAGIC is a demonstration of the API, not a real credential store. The actually-
# MAGIC sensitive artifact for this project is the `.gitignore` file.
# MAGIC
# MAGIC Idempotent: safe to re-run. Uses the Databricks Workspace SDK, which is
# MAGIC pre-installed on Databricks Runtime.

# COMMAND ----------

# We use the Workspace SDK (databricks-sdk) rather than the older
# `dbutils.secrets` API for scope creation, because dbutils only exposes
# READ operations on secrets (`get`, `list`, `listScopes`). Scope/secret
# CREATION must go through the REST API or the SDK that wraps it.
#
# Docs: https://docs.databricks.com/aws/en/dev-tools/sdk-python.html
#       https://databricks-sdk-py.readthedocs.io/en/latest/workspace/workspace/secrets.html

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists

w = WorkspaceClient()  # auto-auths inside Databricks via the notebook context

SCOPE_NAME = "olist-scope"

try:
    w.secrets.create_scope(scope=SCOPE_NAME)
    print(f"✅ Created secret scope: {SCOPE_NAME}")
except ResourceAlreadyExists:
    # Idempotency — re-runs of this notebook should be no-ops, not failures.
    print(f"ℹ️  Scope already exists: {SCOPE_NAME} (no action taken)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the scope

# COMMAND ----------

# `put_secret` upserts: if the key exists, the value is overwritten;
# otherwise it's inserted. There's no separate `update_secret` call.
#
# The bucket name itself is not a secret (it's already in your Phase 0 notes
# and notebooks). We're storing it here purely to demonstrate the
# write → read → reference cycle. In production this would be an API key,
# DB password, or service account JSON.

w.secrets.put_secret(
    scope=SCOPE_NAME,
    key="gcs-bucket-name",
    string_value="<YOUR-SECRET-KEY>",
)
print(f"✅ Put secret: {SCOPE_NAME}/gcs-bucket-name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Put a demonstration secret
# MAGIC

# COMMAND ----------

# Two verification calls:
#   list_scopes() — confirms the scope is registered at workspace level
#   list_secrets(scope) — confirms the key exists in the scope
#
# Note: list_secrets() returns ONLY metadata (key name, last-updated
# timestamp). The actual secret value is never returned by any list call.
# This is by design — secrets are write-once-read-only-via-get.

scopes = [s.name for s in w.secrets.list_scopes()]
print(f"Workspace scopes: {scopes}")
assert SCOPE_NAME in scopes, f"Scope {SCOPE_NAME} not found"

secrets_in_scope = [s.key for s in w.secrets.list_secrets(scope=SCOPE_NAME)]
print(f"Secrets in {SCOPE_NAME}: {secrets_in_scope}")
assert "gcs-bucket-name" in secrets_in_scope, "Expected key not found"

print("\n✅ Phase 8 verification passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify scope and secret exist

# COMMAND ----------

# This is THE pattern to remember for the exam:
#   dbutils.secrets.get(scope=..., key=...)
#
# The returned value is a string. Do not print it or derived values such
# as URLs, paths, substrings, or lengths. Databricks may redact direct
# secret output in notebooks, but safe code should treat the value as
# sensitive once it is in memory.

bucket_name = dbutils.secrets.get(scope="olist-scope", key="gcs-bucket-name")

assert bucket_name, "Secret value should be present"
print("Secret retrieval succeeded")

# COMMAND ----------
