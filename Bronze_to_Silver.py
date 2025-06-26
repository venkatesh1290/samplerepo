# Databricks notebook source
from datetime import datetime

# COMMAND ----------

now=datetime.now()
print(now)

# COMMAND ----------

# Initializing and declaring variables
selected_Schema_Name = 'schema_RemitAmountType'
source_File = 'RemitAmountType.csv'
silver_Delta_Table = 'remitamounttype'

# COMMAND ----------

# Importing required functions
from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run /Users/evokeacntforpoc@gmail.com/POC/mapping_Bronze

# COMMAND ----------

# Function to get schema based on input
def get_schema(schema_name):
    return schemas.get(schema_name)

# COMMAND ----------

# Calling get_schema function
selected_schema = get_schema(selected_Schema_Name)

# COMMAND ----------

Source_File_Name = "dbfs:/FileStore/InputFiles/" + source_File
print(f'source file is',{Source_File_Name})

# COMMAND ----------

# Reading and creating the source dataframe
source_df = spark.read.format("csv").option("header","true").schema(selected_schema).load(Source_File_Name)
#display(source_df)

# COMMAND ----------

target_table_path = "dbfs:/user/hive/warehouse/silver.db/" + silver_Delta_Table
print(f'silver delta table is',{target_table_path})

# COMMAND ----------

from delta.tables import *

target_table = DeltaTable.forPath(spark, target_table_path)

# COMMAND ----------

join_columns = ["RemitAmountTypeCode"]  # List of join columns
join_condition = " AND ".join([f"Target.{col} = Source.{col}" for col in join_columns])

# COMMAND ----------

cols_to_update = {col: "Source." + col for col in dfUpdates.columns if col != "RemitAmountTypeKey"}

# Merge operation
target_table.alias("Target").merge(
    dfUpdates.alias("Source"),
    join_condition
).whenMatchedUpdate(set=cols_to_update) \
 .whenNotMatchedInsert(values=cols_to_update) \
 .execute()


# COMMAND ----------

from delta.tables import *

dfUpdates = source_df

target_table.alias("Target")\
    .merge(
            source = dfUpdates.alias("Source"),
            condition = "Target.RemitAmountTypeCode = Source.RemitAmountTypeCode"
          )\
    .whenMatchedUpdate(
                        set =
                        {
                            "Target.RemitAmountTypeDescription": "Source.RemitAmountTypeDescription"
                            #"Target.LastName": now,
                            #"Target.Country": now
                        }
                      )\
    .whenNotMatchedInsert(
                            values =
                            {
                                "Target.RemitAmountTypeCode": "Source.RemitAmountTypeCode",
                                "Target.RemitAmountTypeDescription": "Source.RemitAmountTypeDescription",
                                "Target.CutOffEffectiveDateTimeUTC": "Source.CutOffEffectiveDateTimeUTC",
                                "Target.CutOffExpirationDateTimeUTC": "Source.CutOffExpirationDateTimeUTC"
                            }
                        )\
    .execute()

# COMMAND ----------

count_df = spark.read.format("delta") \
    .load(target_table_path)
count_df.count()

# COMMAND ----------

display(target_table.history())

# COMMAND ----------

version_number = 5

# Read the Delta table at the specified version
display(spark.read.format("delta") \
    .option("versionAsOf", version_number) \
    .load(target_table_path))
