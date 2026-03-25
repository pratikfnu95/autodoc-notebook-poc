# Databricks notebook source
# Databricks notebook

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Read raw XML-flattened table
src_df = spark.sql("""
    SELECT
        PolicyNumber_newValue,
        PolicyRef_newValue,
        DeckNumber_newValue,
        TransactionEffectiveDate_newValue,
        WrittenPremium_newValue,
        AnnualPremium_newValue,
        State_newValue,
        IsFullyEarned_newValue
    FROM raw.policy_xml
""")

# Step 2: Map XML newValue attributes to business columns
stg_df = (
    src_df
    .select(
        F.col("PolicyNumber_newValue").alias("policy_number"),
        F.col("PolicyRef_newValue").alias("policy_ref"),
        F.col("DeckNumber_newValue").cast("int").alias("deck_number"),
        F.to_date("TransactionEffectiveDate_newValue").alias("transaction_effective_date"),
        F.col("WrittenPremium_newValue").cast("decimal(18,2)").alias("written_premium"),
        F.col("AnnualPremium_newValue").cast("decimal(18,2)").alias("annual_premium"),
        F.col("State_newValue").alias("state"),
        F.when(F.col("IsFullyEarned_newValue") == "Y", F.lit(1)).otherwise(F.lit(0)).alias("is_fully_earned_ind")
    )
    .withColumn("premium_change_amt", F.col("annual_premium") - F.col("written_premium"))
    .withColumn("load_ts", F.current_timestamp())
)

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
