# Databricks notebook source
# MAGIC %md
# MAGIC The code creates three text widgets in Databricks: one for the environment, one for the run type (defaulting to "once"), and one for the microbatch interval (defaulting to "5 seconds").

# COMMAND ----------

dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("RunType", "once", "Set once to run as a batch")
dbutils.widgets.text("ProcessingTime", "5 seconds", "Set the microbatch interval")

# COMMAND ----------

# MAGIC %md
# MAGIC The code retrieves values from the Databricks widgets: Environment, RunType, and ProcessingTime. Based on the value of the RunType widget, it prints either a message indicating that the process will run in batch mode or in stream mode with the specified microbatch interval.

# COMMAND ----------

env = dbutils.widgets.get("Environment")
once = True if dbutils.widgets.get("RunType")=="once" else False
processing_time = dbutils.widgets.get("ProcessingTime")
if once:
    print(f"Starting sbit in batch mode.")
else:
    print(f"Starting sbit in stream mode with {processing_time} microbatch.")

# COMMAND ----------

# MAGIC %md
# MAGIC This code sets several Spark configurations for optimizing the performance and behavior of Delta Lake and streaming operations. Specifically:
# MAGIC - It sets the shuffle partitions to the default parallelism value.
# MAGIC - Enables Delta Lake's optimized write and auto-compaction features.
# MAGIC - Configures the streaming state store to use the RocksDBStateStoreProvider for more efficient state management in streaming queries.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %run ./load_cutover_data

# COMMAND ----------

SH = SetupHelper(env)
HL = HistoryLoader(env)

# COMMAND ----------

setup_required = spark.sql(f"SHOW DATABASES IN {SH.catalog}").filter(f"databaseName == '{SH.db_name}'").count() != 1
if setup_required:
    SH.setup()
    SH.validate()
    HL.load_history()
    HL.validate()
else:
    spark.sql(f"USE {SH.catalog}.{SH.db_name}")

# COMMAND ----------

# MAGIC %run ./bronze

# COMMAND ----------

# MAGIC %run ./silver

# COMMAND ----------

# MAGIC %run ./gold

# COMMAND ----------

BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)

# COMMAND ----------

BZ.consume(once, processing_time)

# COMMAND ----------

SL.upsert(once, processing_time)

# COMMAND ----------

GL.upsert(once, processing_time)
