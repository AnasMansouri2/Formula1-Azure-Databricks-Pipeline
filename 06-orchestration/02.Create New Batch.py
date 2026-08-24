# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Create New Batch

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

if v_batch_id:
    in_progress_df = (
        spark.createDataFrame(
            [Row(batch_id=v_batch_id, status="in_progress")]
        )
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )

    (
        in_progress_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(control_table)
    )

    print(f"Marked batch {v_batch_id} as in_progress")
else:
    raise Exception("batch_id is missing")  
