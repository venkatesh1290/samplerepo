# Databricks notebook source
df=spark.read.csv("/FileStore/tables/employee-3.csv",header=True,inferSchema=True)
display(df)

# COMMAND ----------

df.select("name","salary").show()

# COMMAND ----------

df.select("name","location","email").show()

# COMMAND ----------

df.select(df.name,df.salary).show()

# COMMAND ----------

df.select(df.columns[1:5]).show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.withColumn("salary_new",df.salary*2).show()

# COMMAND ----------

from pyspark.sql.functions import lit
df.withColumn("country",lit("usa")).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df1=df.withColumn("salary",col("salary").cast("string"))
df1.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.withColumnRenamed("dept","department").show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.filter(df.name=='chinna').show()

# COMMAND ----------

df.filter(df.name!='chinna').show()

# COMMAND ----------

df.filter((df.name=='chinna') & (df.salary==10000)).show()

# COMMAND ----------

df.filter((df.name=='chinna') | (df.salary==20000)).show()

# COMMAND ----------

df.filter(df.name.startswith("ch")).show()

# COMMAND ----------

df.filter(df.name.endswith("an")).show()

# COMMAND ----------

display(df)

# COMMAND ----------

df1=df.distinct()
df1.show()

# COMMAND ----------

df2=df.dropDuplicates(['name','salary'])
df2.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.sort("salary").show()

# COMMAND ----------

df.orderBy("salary").show()

# COMMAND ----------

df.sort("salary","location").show()

# COMMAND ----------

df.sort(df.salary.desc()).show()

# COMMAND ----------

df.orderBy(df.salary.desc()).show()

# COMMAND ----------

df.sort("salary".desc()).show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("dept").sum("salary").show()


# COMMAND ----------

df.groupBy("dept").count().show()

# COMMAND ----------

df.groupBy("dept","location").sum("salary").show()

# COMMAND ----------

df.groupBy("dept").max("salary").show()

# COMMAND ----------


