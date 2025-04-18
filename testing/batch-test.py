# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %run ../src/setup

# COMMAND ----------

SH = SetupHelper(env)
SH.cleanup()

# COMMAND ----------

dbutils.notebook.run("../src/run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

# MAGIC %run ../src/load_cutover_data

# COMMAND ----------

HL = HistoryLoader(env)
SH.validate()
HL.validate()

# COMMAND ----------

# MAGIC %run ../src/producer

# COMMAND ----------

PR =Producer()
PR.produce(1)
PR.validate(1)
dbutils.notebook.run("../src/run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

# MAGIC %run ../src/bronze

# COMMAND ----------

# MAGIC %run ../src/silver

# COMMAND ----------

# MAGIC %run ../src/gold

# COMMAND ----------

BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)
BZ.validate(1)
SL.validate(1)
GL.validate(1)

# COMMAND ----------

PR.produce(2)
PR.validate(2)
dbutils.notebook.run("../src/run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

BZ.validate(2)
SL.validate(2)
GL.validate(2)
SH.cleanup()
