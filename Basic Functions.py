# Databricks notebook source
# DBTITLE 1,Creating a folder in DBFS to store Input Files
dbutils.fs.mkdirs("dbfs:/FileStore/InputFiles")

# COMMAND ----------

# DBTITLE 1,Listing the files/folders available in the given path
display(dbutils.fs.ls("dbfs:/FileStore/InputFiles"))

# COMMAND ----------

# DBTITLE 1,Removing selected file
dbutils.fs.rm("dbfs:/FileStore/InputFiles/Information.csv",True)

# COMMAND ----------

# DBTITLE 1,Create the schema for the file
 from pyspark.sql.types import *

 definedSchema = StructType([
     StructField("id", IntegerType()),
     StructField("name", StringType()),
     StructField("dept", DateType()),
     StructField("location", StringType()),
     StructField("joiningdate", StringType()),
     StructField("joiningfee", FloatType()),
     StructField("IsActive", BooleanType())
     ])

# COMMAND ----------

# DBTITLE 1,Reading and Displaying file data
source_df = spark.read.format("csv").option("header","true").schema(definedSchema).load("dbfs:/FileStore/InputFiles/Information.csv")
display(source_df)

# COMMAND ----------

source_df.describe()

# COMMAND ----------

# DBTITLE 1,Adding Columns to source dataframes
 from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name

 transform_df = source_df.withColumn("FileName", input_file_name()) \
     .withColumn("IsFlagged", when(col("joiningdate") < '2023-12-31',True).otherwise(False)) \
     .withColumn("CreatedTS", current_timestamp()) \
     .withColumn("ModifiedTS", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Case Statement in Spark
 transform_df = transform_df.withColumn("name", when((col("name").isNull() | (col("name")=="")),lit("Unknown")).otherwise(col("name"))) \
     .withColumn("dept", when((col("dept").isNull() | (col("dept")=="")),lit("No dept")).otherwise(col("dept")))
 display(transform_df)

# COMMAND ----------

# DBTITLE 1,Delta table creation
 from pyspark.sql.types import *
 from delta.tables import *

 DeltaTable.createIfNotExists(spark) \
     .tableName("information_silver") \
     .addColumn("id", IntegerType()) \
     .addColumn("name", StringType()) \
     .addColumn("dept", DateType()) \
     .addColumn("location", StringType()) \
     .addColumn("joiningdate", StringType()) \
     .addColumn("joiningfee", FloatType()) \
     .addColumn("IsActive", BooleanType()) \
     .addColumn("FileName", StringType()) \
     .addColumn("IsFlagged", BooleanType()) \
     .addColumn("CreatedTS", DateType()) \
     .addColumn("ModifiedTS", DateType()) \
     .execute()

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, 'Tables/information_silver')
    
dfUpdates = transform_df


deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.SalesOrderNumber = updates.SalesOrderNumber and silver.OrderDate = updates.OrderDate and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "SalesOrderNumber": "updates.SalesOrderNumber",
      "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
      "OrderDate": "updates.OrderDate",
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "Item": "updates.Item",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax",
      "FileName": "updates.FileName",
      "IsFlagged": "updates.IsFlagged",
      "CreatedTS": "updates.CreatedTS",
      "ModifiedTS": "updates.ModifiedTS"
    }
  ) \
  .execute()
