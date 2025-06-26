# Databricks notebook source
data=[(1,"chinna@gmail.com"),(2,"rangi@gmail.com"),(1,"chinna1@gmail.com")]
schema=["id","mail"]

df=spark.createDataFrame(data,schema)
df = df.dropDuplicates(["id",])
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
df.groupBy("mail").count().filter(col('count')>1).show()

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample

# COMMAND ----------

# MAGIC %sql
# MAGIC select mail,count(*) from sample group by mail having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct id,mail from sample

# COMMAND ----------

df.show()

# COMMAND ----------

df.distinct().show()

# COMMAND ----------

data=[(1,"chinnna"),(2,"ramu"),(3,"sitha"),(4,"bhanu")]
schema=["customer_id","customer_name"]

df_customer=spark.createDataFrame(data,schema)
df_customer.show()

data1=[(1,4),(3,2)]
schema1=["order_id","customer_id"]


df_order=spark.createDataFrame(data1,schema1)
df_order.show()

# COMMAND ----------

df_customer.join(df_order,df_customer.customer_id==df_order.customer_id,"inner").show()

# COMMAND ----------

df_customer.createOrReplaceTempView("customer")
df_order.createOrReplaceTempView("order")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order

# COMMAND ----------

df_customer.createOrReplaceTempView("customer")

# COMMAND ----------

df_order.createOrReplaceTempView("order")

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select * from customer left join order on customer.customer_id=order.customer_id )
# MAGIC select * from cte where order_id is null

# COMMAND ----------

data=[(1,),(2,),(3,),(4,),(7,),(10,)]
schema=["id"]
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.types import IntegerType
list_new=range(1,11,1)
df_new=spark.createDataFrame(list_new,IntegerType())
df_new.show()

# COMMAND ----------

df_new.subtract(df).show()

# COMMAND ----------

data=[("manish","{'street':'123 st','city':'delhi'}"),
      ("ram","{'street':'432 st','city':'pune'}")]
schema=["name","location"]
df=spark.createDataFrame(data,schema)
df.printSchema()
display(df)

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,location,from_json(location,'street string,city string') as location_new from sample 

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select name,location,from_json(location,'street string,city string') as location_new from sample 
# MAGIC )
# MAGIC select name,location,location_new,location_new.street as street,location_new.city as city from cte

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("location_new",from_json(col("location"),'street string,city string'))

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.select("name","location","location_new",col('location_new').street.alias("street"),col('location_new').city.alias("city")).show()

# COMMAND ----------

data=[('202-01-01',20000),('2024-01-02',10000),('2024-01-03',30000),('2024-01-04',25000),('2024-01-05',40000)]
schema=["date","sales"]

df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,sum(sales) over(order by date) as cummulative_sales,
# MAGIC lag(sales) over(order by date) as previous_sales 
# MAGIC ,lead(sales) over(order by date) as next_sales from sample

# COMMAND ----------


