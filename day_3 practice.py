# Databricks notebook source
data=[(1,"chinna",10000,"india"),(2,"sreenu",20000,"uk"),(3,"ramya",15000,"india")]
schema=["id","name","salary","country"]
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

data1=[(1,"it","tcs"),(2,"java","techm"),(4,"sql","infosis")]
schema1=["course","depatment","company"]
df1=spark.createDataFrame(data1,schema1)
df1.show()


# COMMAND ----------

employee_tbl=df
df.show()
depatment_tbl=df1
df1.show()

# COMMAND ----------

employee_tbl.join(depatment_tbl,employee_tbl.id==depatment_tbl.course,"leftanti").show()



# COMMAND ----------

df=spark.read.csv("/FileStore/tables/sample.csv",header=True,inferSchema=True)
display(df)

# COMMAND ----------

df.na.fill(" ").show()

# COMMAND ----------

sample=spark.read.csv("/FileStore/tables/sample.csv",header=True)
sample.show()

# COMMAND ----------

sample.printSchema()

# COMMAND ----------

sample.na.fill(" ").show()

# COMMAND ----------

sample1=spark.read.csv("/FileStore/tables/sample.csv",header=True)
sample1.show()

# COMMAND ----------

df.na.fill("").show()

# COMMAND ----------


