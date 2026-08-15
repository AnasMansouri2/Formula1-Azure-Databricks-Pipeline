# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Transform Drivers Data
# MAGIC
# MAGIC 1. Read bronze `drivers` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`driverId` → `driver_id`, `dateOfbirth` → `date_of_birth`)
# MAGIC 1. Concatenate `name.givenName` and `name.familyName` to create a new column called `driver_name` and transform the value to Title Case
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of column `nationality` to Title Case
# MAGIC 1. Write the transformed data to silver `drivers` table
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")  

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze `drivers` table

# COMMAND ----------

drivers_df = (
    spark.table(bronze_table)
         .filter((F.col("batch_id") == v_batch_id))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

drivers_dropped_df = drivers_df.drop(F.col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Standardise Column Names
# MAGIC - Standardise column names using snake_case (`driverId` → `driver_id`, `dateOfBirth` → `date_of_birth`)

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
        .withColumnsRenamed({
            "driverId": "driver_id",
            "dateOfBirth": "date_of_birth"
        })
)

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Concatenate name.givenName and name.familyName to create a new column called driver_name

# COMMAND ----------

drivers_concatenated_df = (
  drivers_renamed_df
       .withColumn("driver_name", 
                   F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
       .drop("name")
)

# COMMAND ----------

display(drivers_concatenated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Remove duplicate records

# COMMAND ----------

drivers_distinct_df = drivers_concatenated_df.dropDuplicates(["driver_id"])

# COMMAND ----------

display(drivers_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Transform values of column `nationality` to Title Case

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
        .withColumn('nationality', F.initcap(F.col("nationality")))
)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Write the transformed data to silver `drivers` table

# COMMAND ----------

write_to_silver(
    input_df=drivers_final_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))
