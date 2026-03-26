# Databricks notebook source
# Databricks Notebook

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ./temp_view

# COMMAND ----------

stg_df = spark.sql("""
    SELECT
        PolicyNumber_newValue AS policy_number,
        PolicyRef_newValue AS policy_ref,
        CAST(DeckNumber_newValue AS INT) AS deck_number,
        CAST(TransactionEffectiveDate_newValue AS DATE) AS transaction_effective_date,
        CAST(WrittenPremium_newValue AS DECIMAL(18,2)) AS written_premium,
        CAST(AnnualPremium_newValue AS DECIMAL(18,2)) AS annual_premium,
        State_newValue AS state,
        CASE
            WHEN IsFullyEarned_newValue = 'Y' THEN 1
            ELSE 0
        END AS is_fully_earned_ind,
        CAST(AnnualPremium_newValue AS DECIMAL(18,2)) -
        CAST(WrittenPremium_newValue AS DECIMAL(18,2)) AS premium_change_amt,
        current_timestamp() AS load_ts
    FROM vw_policy_xml_raw
""")

# Step 3: Deduplicate latest record by policy and transaction date
window_spec = (
    Window
    .partitionBy("policy_number", "transaction_effective_date")
    .orderBy(F.col("load_ts").desc())
)

final_df = (
    stg_df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

final_df.createOrReplaceTempView("vw_policy_transaction_stage")



# COMMAND ----------

# MAGIC  %sql
# MAGIC  -- Step 4: Insert or merge into downstream transaction table
# MAGIC  MERGE INTO mart.policy_transaction t
# MAGIC  USING vw_policy_transaction_stage s
# MAGIC  ON t.policy_number = s.policy_number
# MAGIC  AND t.transaction_effective_date = s.transaction_effective_date
# MAGIC  WHEN MATCHED THEN
# MAGIC    UPDATE SET
# MAGIC      t.policy_ref = s.policy_ref,
# MAGIC      t.deck_number = s.deck_number,
# MAGIC      t.written_premium = s.written_premium,
# MAGIC      t.annual_premium = s.annual_premium,
# MAGIC      t.premium_change_amt = s.premium_change_amt,
# MAGIC      t.state = s.state,
# MAGIC      t.is_fully_earned_ind = s.is_fully_earned_ind,
# MAGIC      t.load_ts = s.load_ts
# MAGIC  WHEN NOT MATCHED THEN
# MAGIC    INSERT (
# MAGIC      policy_number,
# MAGIC      policy_ref,
# MAGIC      deck_number,
# MAGIC      transaction_effective_date,
# MAGIC      written_premium,
# MAGIC      annual_premium,
# MAGIC      premium_change_amt,
# MAGIC      state,
# MAGIC      is_fully_earned_ind,
# MAGIC      load_ts
# MAGIC    )
# MAGIC    VALUES (
# MAGIC      s.policy_number,
# MAGIC      s.policy_ref,
# MAGIC      s.deck_number,
# MAGIC      s.transaction_effective_date,
# MAGIC      s.written_premium,
# MAGIC      s.annual_premium,
# MAGIC      s.premium_change_amt,
# MAGIC      s.state,
# MAGIC      s.is_fully_earned_ind,
# MAGIC      s.load_ts
# MAGIC    )
