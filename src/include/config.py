# Databricks notebook source
# MAGIC %md
# MAGIC This code sets up a config object that stores the locations of your data and checkpoints, the name of the database you'll use, and a limit for how many files to process at a time. It's basically pulling info from your Spark setup to get everything ready for processing.

# COMMAND ----------

class Config():    
    def __init__(self):      
        self.base_dir_data = spark.sql("describe external location `data_zone`").select("url").collect()[0][0]
        self.base_dir_checkpoint = spark.sql("describe external location `checkpoint`").select("url").collect()[0][0]
        self.db_name = "sbit_db"
        self.maxFilesPerTrigger = 1000
