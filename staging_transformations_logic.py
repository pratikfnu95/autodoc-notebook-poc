# Databricks notebook source
# Databricks notebook

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
    FROM policy_xml
""")

# Step 2: Create temp view from source dataframe
src_df.createOrReplaceTempView("vw_policy_xml_raw")

# COMMAND ----------

# Databricks notebook

# Step 1: Read raw XML-flattened table
src_df2 = spark.sql("""
    SELECT
        AccountNumber_newValue,
        PolicyRef_newValue,
        Producercode_newValue
    FROM policy_xml
""")

# Step 2: Create temp view from source dataframe
src_df2.createOrReplaceTempView("vw_account_xml_raw")
