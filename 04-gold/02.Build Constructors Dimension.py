# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Build Constructors Dimension
# MAGIC
# MAGIC 1. Read silver `constructors` table
# MAGIC 1. Read gold `ref_nationality_region` table
# MAGIC 1. Join the data from `constructors` with `ref_nationality_region` using `nationality`
# MAGIC 1. Select the required columns
# MAGIC     - constructors.constructor_id
# MAGIC     - constructors.constructor_name
# MAGIC     - constructors.nationality
# MAGIC     - ref_nationality_region.region
# MAGIC 1. Write the transformed data to gold `dim_constructors` table
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read source tables
# MAGIC - `silver.constructors`
# MAGIC - `gold.ref_nationality_region`

# COMMAND ----------

constructors_df = (
    spark.table(f"{catalog_name}.{silver_schema}.constructors")
         .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------

ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Join `constructors` with `nationality_region_df` using `nationality`
# MAGIC Select the following columns   
# MAGIC 1. constructors.constructor_id 
# MAGIC 1. constructors.constructor_name 
# MAGIC 1. constructors.nationality 
# MAGIC 1. ref_nationality_region.region

# COMMAND ----------

dim_constructors_df = (
    constructors_df
        .join(
            ref_nationality_region_df,
            constructors_df.nationality == ref_nationality_region_df.nationality,
            "left"
            )
        .select (
            constructors_df.constructor_id,
            constructors_df.constructor_name,
            constructors_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write the transformed data to the `gold` `dim_constructors` table

# COMMAND ----------

write_to_gold(
    input_df=dim_constructors_df,
    target_table=target_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "nationality_region"
    ]
)

# COMMAND ----------

display(spark.table(target_table))
