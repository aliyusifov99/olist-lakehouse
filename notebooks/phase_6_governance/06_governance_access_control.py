# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Phase 6 — Access Control: Patterns and Working Demos
# MAGIC
# MAGIC Single-user workspace, but four governance mechanics ARE demonstrable:
# MAGIC
# MAGIC 1. SHOW GRANTS / information_schema.table_privileges (audit)
# MAGIC 2. Self-revoke / self-grant cycle (mechanism is real and reversible)
# MAGIC 3. Column mask on silver.customers.customer_unique_id (live, working)
# MAGIC 4. Row filter on silver.customers by customer_state (live, working)
# MAGIC
# MAGIC Plus the patterns that extend (3) and (4) to groups via IS_MEMBER(),
# MAGIC documented as commented SQL.
# MAGIC
# MAGIC References:
# MAGIC - SHOW GRANTS:
# MAGIC   https://docs.databricks.com/aws/en/sql/language-manual/security-show-grant
# MAGIC - Row filters and column masks:
# MAGIC   https://docs.databricks.com/aws/en/data-governance/unity-catalog/row-and-column-filters
# MAGIC - IS_MEMBER:
# MAGIC   https://docs.databricks.com/aws/en/sql/language-manual/functions/is_member

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit current grants

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What does my user currently have on the catalog and its schemas?
# MAGIC -- SHOW GRANTS surfaces effective privileges including ones inherited
# MAGIC -- from owner status (an OWNER has all privileges implicitly).
# MAGIC
# MAGIC SHOW GRANTS ON CATALOG olist_lakehouse_us;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA olist_lakehouse_us.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The same data, queryable instead of just printable. table_privileges
# MAGIC -- is a per-catalog SQL-spec view exposing object grants.
# MAGIC -- Reference: https://docs.databricks.com/aws/en/sql/language-manual/information-schema/table_privileges
# MAGIC
# MAGIC SELECT
# MAGIC   grantor,
# MAGIC   grantee,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   privilege_type,
# MAGIC   is_grantable
# MAGIC FROM olist_lakehouse_us.information_schema.table_privileges
# MAGIC WHERE table_schema IN ('bronze', 'silver', 'gold')
# MAGIC ORDER BY table_schema, table_name, privilege_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demonstrate the grant mechanism by revoking and re-granting on a
# MAGIC -- Bronze table. We use bronze.geolocation because it's reference data
# MAGIC -- (no downstream consumer, no dashboard), so a brief revoke window is
# MAGIC -- safe. The cycle proves the DDL is real and reversible.
# MAGIC
# MAGIC -- Step 1: confirm we can read the table
# MAGIC SELECT COUNT(*) AS row_count FROM olist_lakehouse_us.bronze.geolocation;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Self-revoke / self-grant cycle

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: revoke SELECT from a demo principal.
# MAGIC -- Note: this revokes EXPLICIT SELECT. If the user is the table owner,
# MAGIC -- the implicit OWNERSHIP-derived SELECT remains, and queries still work.
# MAGIC -- For a clean demo we use a Bronze table whose ownership we don't rely
# MAGIC -- on for downstream work.
# MAGIC -- Replace `<YOUR-EMAIL>` with a test principal in a private workspace,
# MAGIC -- or use a group such as `data_engineers` for production governance.
# MAGIC
# MAGIC -- REVOKE SELECT ON TABLE olist_lakehouse_us.bronze.geolocation FROM `<YOUR-EMAIL>`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: re-grant SELECT
# MAGIC -- GRANT SELECT ON TABLE olist_lakehouse_us.bronze.geolocation TO `<YOUR-EMAIL>`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: verify the grant landed
# MAGIC SHOW GRANTS ON TABLE olist_lakehouse_us.bronze.geolocation;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column mask (live, working)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A column mask is a UC-managed function that takes the original
# MAGIC -- column value and returns the value the querier should see. The
# MAGIC -- function is enforced by the engine on every SELECT.
# MAGIC
# MAGIC -- We define the mask in the silver schema (same schema as the table)
# MAGIC -- and write it for the demonstrable case: any non-superuser sees
# MAGIC -- the value masked. In production the IS_MEMBER('admins') branch
# MAGIC -- would let admins see the real value; here, current_user happens to
# MAGIC -- be the catalog owner and sees the unmasked value via owner override.
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION olist_lakehouse_us.silver.mask_customer_id(uid STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC DETERMINISTIC
# MAGIC COMMENT 'Column mask for customer_unique_id. Returns first 4 chars + ***. In production, branch on IS_MEMBER(''pii_readers'') to expose unmasked value to authorized groups.'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('pii_readers') THEN uid
# MAGIC     ELSE CONCAT(SUBSTRING(uid, 1, 4), '***')
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attach the mask to the column. ALTER ... SET MASK is the syntax
# MAGIC -- that binds the function to the column.
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.customers
# MAGIC   ALTER COLUMN customer_unique_id SET MASK olist_lakehouse_us.silver.mask_customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demonstrate: query the masked column. Since you're not a member of
# MAGIC -- the 'pii_readers' group (it doesn't exist in this workspace), you'll
# MAGIC -- see the masked form: first 4 chars + '***'.
# MAGIC
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_unique_id,
# MAGIC   customer_state
# MAGIC FROM olist_lakehouse_us.silver.customers
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row filter (live, working)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A row filter is a UC-managed function that takes one or more
# MAGIC -- column values and returns a BOOLEAN. The engine evaluates the
# MAGIC -- function per-row on SELECT; rows where it returns FALSE are
# MAGIC -- silently filtered out.
# MAGIC
# MAGIC -- We restrict to non-SP rows. For a multi-user setup, a typical
# MAGIC -- pattern is "users in the ''sp_only'' group see only SP rows" —
# MAGIC -- demonstrated as IS_ACCOUNT_GROUP_MEMBER below.
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION olist_lakehouse_us.silver.filter_customers_by_state(state STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC DETERMINISTIC
# MAGIC COMMENT 'Row filter for customers. Members of the ''sp_only_analysts'' group see only SP rows; everyone else sees all rows. Demo pattern; production would key on a different group/state combination.'
# MAGIC RETURN
# MAGIC   IS_ACCOUNT_GROUP_MEMBER('sp_only_analysts') = false  -- non-members see all rows
# MAGIC   OR state = 'SP';                                      -- members see only SP rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attach the filter. Note the ON column list — this passes
# MAGIC -- customer_state to the filter function as its single argument.
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.customers
# MAGIC   SET ROW FILTER olist_lakehouse_us.silver.filter_customers_by_state ON (customer_state);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demonstrate: count rows by state. Since you're not in the
# MAGIC -- 'sp_only_analysts' group, the filter passes through and you see
# MAGIC -- all 27 states.
# MAGIC
# MAGIC SELECT customer_state, COUNT(*) AS row_count
# MAGIC FROM olist_lakehouse_us.silver.customers
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY row_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multi-user grant patterns (documented)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The grant patterns that would extend the demos above to a real
# MAGIC -- multi-user workspace. Commented out because no groups exist; left
# MAGIC -- as documentation. Match each pattern to the cert exam expectation.
# MAGIC
# MAGIC -- Pattern 1: catalog/schema/table read access for an analyst group
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- GRANT USE CATALOG ON CATALOG olist_lakehouse_us TO `analysts`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA olist_lakehouse_us.gold TO `analysts`;
# MAGIC -- GRANT SELECT ON SCHEMA olist_lakehouse_us.gold TO `analysts`;
# MAGIC
# MAGIC -- Pattern 2: explicitly DENY access to the lower layers
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- (UC doesn't have explicit DENY; absence of grant IS denial. So
# MAGIC -- the analyst group simply gets no grant on bronze/silver. This
# MAGIC -- is the cleanest pattern — implicit denial via no-grant.)
# MAGIC
# MAGIC -- Pattern 3: PII-readers subgroup with mask bypass
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- The mask function in Pattern 3 above already branches on
# MAGIC -- IS_ACCOUNT_GROUP_MEMBER('pii_readers'). To make that real:
# MAGIC -- GRANT SELECT ON TABLE olist_lakehouse_us.silver.customers TO `pii_readers`;
# MAGIC -- (Members of pii_readers would see the unmasked customer_unique_id;
# MAGIC -- everyone else with SELECT on the table sees the masked form.)
# MAGIC
# MAGIC -- Pattern 4: row-filter targeted group
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- The row filter on IS_ACCOUNT_GROUP_MEMBER('sp_only_analysts').
# MAGIC -- A real workspace would create the group and the grant:
# MAGIC -- GRANT SELECT ON TABLE olist_lakehouse_us.silver.customers TO `sp_only_analysts`;
# MAGIC
# MAGIC -- Pattern 5: ownership transfer (production governance hygiene)
# MAGIC -- ----------------------------------------------------------------
# MAGIC -- ALTER CATALOG olist_lakehouse_us OWNER TO `data_platform_admins`;
# MAGIC -- (Group ownership instead of individual ownership prevents access
# MAGIC -- loss when an individual leaves the org.)
