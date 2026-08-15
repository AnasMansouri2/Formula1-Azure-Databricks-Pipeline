# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Build Races Dimension
# MAGIC
# MAGIC 1. Read silver `races` table
# MAGIC 1. Read silver `circuits` table
# MAGIC 1. Join the data from `races` with `circuits` using `circuit_id`
# MAGIC 1. Select the required columns
# MAGIC     - races.season 
# MAGIC     - races.round 
# MAGIC     - races.race_name 
# MAGIC     - races.race_date 
# MAGIC     - circuits.circuit_name 
# MAGIC     - circuits.locality 
# MAGIC     - circuits.country
# MAGIC 1. Write the transformed data to gold `dim_races` table
# MAGIC
# MAGIC > Below changes are required to implement Incremental Load Processing
# MAGIC 1. Accept batch_id as a parameter to the notebook
# MAGIC 1. Process data for only the batch_id being passed in (i.e., filter reading from silver using the batch_id)
# MAGIC 1. Add created_timestamp, updated_timestamp to the gold table. 
# MAGIC 1. Merge the processed data to the gold table
# MAGIC     - created_timestamp should only be populated at the time of inserting/ creating the record. It should not be updated during the merge update.

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read source tables
# MAGIC - `circuits`
# MAGIC - `races`

# COMMAND ----------

circuits_df = (
    spark.table(f"{catalog_name}.{silver_schema}.circuits")
         .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------


races_df = (
    spark.table(f"{catalog_name}.{silver_schema}.races")
         .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Join `races` with `circuits` using `circuit_id`
# MAGIC Select the following columns  
# MAGIC   1. races.season 
# MAGIC   1. races.round 
# MAGIC   1. races.race_name 
# MAGIC   1. races.race_date 
# MAGIC   1. circuits.circuit_name 
# MAGIC   1. circuits.locality 
# MAGIC   1. circuits.country

# COMMAND ----------

dim_races_df = (
            races_df
                .join(
                    circuits_df,
                    races_df.circuit_id == circuits_df.circuit_id,
                    "inner"
                )
                .select (
                    races_df.season,
                    races_df.round,
                    races_df.race_name,
                    races_df.race_date,
                    circuits_df.circuit_name,
                    circuits_df.locality,
                    circuits_df.country
                )
        )

# COMMAND ----------

display(dim_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write the transformed data to the `gold` `dim_races` table

# COMMAND ----------

write_to_gold(
    input_df=dim_races_df,
    target_table=target_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_name",
        "locality",
        "country"
    ]
)

# COMMAND ----------

display(spark.table(target_table))
