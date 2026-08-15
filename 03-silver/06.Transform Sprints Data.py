# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Transform Sprints Data
# MAGIC 1. Read bronze `sprints` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`constructorId` → `constructor_id`, `driverId` → `driver_id`, `raceName` → `race_name`, `positionText` → `finish_position_text`)
# MAGIC 1. Rename columns to make them more meaningful (`date` → `race_date`, `grid` → `grid_position`, `laps` → `completed_laps`, `number` → `car_number`, `position` → `finish_position`)
# MAGIC 1. Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of column `race_name` to Title Case
# MAGIC 1. Write the transformed data to silver `sprints` table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers 

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 to 4 Read Source Data, Select required columns & Standardise column names

# COMMAND ----------

sprints_df = (
  spark.table(bronze_table)
       .filter((F.col("batch_id") == v_batch_id))
       .select("season",
              "round",
              "constructorId",
              "driverId",
              "date",
              "raceName",
              "grid",
              "laps",
              "number",
              "points",
              "position",
              "positionText",
              "status",
              "ingestion_timestamp",
              "source_file",
              "batch_id")
       .withColumnsRenamed({
            "constructorId": "constructor_id",
            "driverId": "driver_id",
            "raceName": "race_name",
            "date": "race_date",
            "grid": "grid_position",
            "laps": "completed_laps",
            "number": "car_number",
            "position": "final_position",
            "positionText": "final_position_text"
        })
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 & 6 Data Quality Checks
# MAGIC - Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)
# MAGIC - Remove duplicate records

# COMMAND ----------

sprints_valid_df = (
    sprints_df
        .filter(
            F.col("season").isNotNull() &
            F.col("round").isNotNull() &
            F.col("constructor_id").isNotNull() &
            F.col("driver_id").isNotNull() 
        )
        .dropDuplicates(["season", "round", "constructor_id", "driver_id"])
)

# COMMAND ----------

display(sprints_df.count() - sprints_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Transform values of column `nationality` to Title Case

# COMMAND ----------

sprints_final_df = (
    sprints_valid_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Write the transformed data to silver `sprints` table

# COMMAND ----------

write_to_silver(
    input_df=sprints_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
    columns_to_update=[
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))
