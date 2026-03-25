# Databricks notebook source
# Databricks Notebook

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Read raw XML-consumed Delta table
raw_df = spark.sql("""
    SELECT
        PolicyNumber,
        DeckNumber,
        EffectiveDate,
        TransactionEffectiveDate,
        WrittenPremium,
        AnnualPremium,
        FinalPremium,
        State,
        LOB
    FROM raw.policy_xml
""")

# Step 2: Apply business-friendly column mapping and basic transformations
transformed_df = (
    raw_df
    .withColumnRenamed("PolicyNumber", "policy_number")
    .withColumnRenamed("DeckNumber", "deck_number")
    .withColumnRenamed("EffectiveDate", "policy_effective_date")
    .withColumnRenamed("TransactionEffectiveDate", "transaction_effective_date")
    .withColumnRenamed("WrittenPremium", "written_premium")
    .withColumnRenamed("AnnualPremium", "annual_premium")
    .withColumnRenamed("FinalPremium", "final_premium")
    .withColumnRenamed("State", "state")
    .withColumnRenamed("LOB", "line_of_business")
    .withColumn("policy_effective_date", F.to_date("policy_effective_date"))
    .withColumn("transaction_effective_date", F.to_date("transaction_effective_date"))
    .withColumn("written_premium", F.col("written_premium").cast("decimal(18,2)"))
    .withColumn("annual_premium", F.col("annual_premium").cast("decimal(18,2)"))
    .withColumn("final_premium", F.col("final_premium").cast("decimal(18,2)"))
    .withColumn("premium_variance", F.col("final_premium") - F.col("written_premium"))
    .withColumn("load_timestamp", F.current_timestamp())
)

# Step 3: Deduplicate records by policy number and transaction effective date
window_spec = Window.partitionBy("policy_number", "transaction_effective_date") \
                    .orderBy(F.col("load_timestamp").desc())

dedup_df = (
    transformed_df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Step 4: Create temp view for SQL merge
dedup_df.createOrReplaceTempView("vw_policy_transaction_stage")


# COMMAND ----------

# MAGIC %sql
# MAGIC  -- Step 5: Upsert into downstream policy transaction table
# MAGIC  MERGE INTO mart.policy_transaction t
# MAGIC  USING vw_policy_transaction_stage s
# MAGIC  ON t.policy_number = s.policy_number
# MAGIC     AND t.transaction_effective_date = s.transaction_effective_date
# MAGIC  WHEN MATCHED THEN
# MAGIC    UPDATE SET
# MAGIC      t.deck_number = s.deck_number,
# MAGIC      t.policy_effective_date = s.policy_effective_date,
# MAGIC      t.written_premium = s.written_premium,
# MAGIC      t.annual_premium = s.annual_premium,
# MAGIC      t.final_premium = s.final_premium,
# MAGIC      t.premium_variance = s.premium_variance,
# MAGIC      t.state = s.state,
# MAGIC      t.line_of_business = s.line_of_business,
# MAGIC      t.load_timestamp = s.load_timestamp
# MAGIC  WHEN NOT MATCHED THEN
# MAGIC    INSERT (
# MAGIC      policy_number,
# MAGIC      deck_number,
# MAGIC      policy_effective_date,
# MAGIC      transaction_effective_date,
# MAGIC      written_premium,
# MAGIC      annual_premium,
# MAGIC      final_premium,
# MAGIC      premium_variance,
# MAGIC      state,
# MAGIC      line_of_business,
# MAGIC      load_timestamp
# MAGIC    )
# MAGIC    VALUES (
# MAGIC      s.policy_number,
# MAGIC      s.deck_number,
# MAGIC      s.policy_effective_date,
# MAGIC      s.transaction_effective_date,
# MAGIC      s.written_premium,
# MAGIC      s.annual_premium,
# MAGIC      s.final_premium,
# MAGIC      s.premium_variance,
# MAGIC      s.state,
# MAGIC      s.line_of_business,
# MAGIC      s.load_timestamp
# MAGIC    )
