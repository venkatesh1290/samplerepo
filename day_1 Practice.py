# Databricks notebook source
10+20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_test

# COMMAND ----------

# DBTITLE 1,creating the data frame
df=spark.read.csv('/FileStore/tables/employee-2.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,creating the dataframe with header
df1=spark.read.csv('/FileStore/tables/employee-2.csv',header=True)
display(df1)

# COMMAND ----------

# DBTITLE 1,displaying the df1
df1.show()

# COMMAND ----------

# DBTITLE 1,printing the schema
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,creating the dataframe with proper header and schema
df2=spark.read.csv('/FileStore/tables/employee-2.csv',header=True,inferSchema=True)
display(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

# DBTITLE 1,Alternate method
df3=spark.read.format('csv').options(header=True,inferSchema=True).load('/FileStore/tables/employee-2.csv')

# COMMAND ----------

display(df3)

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

# DBTITLE 1,reading the json files
df4=spark.read.json("/FileStore/tables/details.json")
display(df4)

# COMMAND ----------

df5=spark.read.json("/FileStore/tables/reason.json",multiLine=True)

# COMMAND ----------

display(df5)

# COMMAND ----------


