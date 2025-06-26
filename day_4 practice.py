# Databricks notebook source
# DBTITLE 1,structType and structField
data=[(1,"chinna","usa"),(2,"ramu","usa"),(1,"muni","usa")]
schema=["id","name","location"]
df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

data=[(1,"chinna","usa"),(2,"ramu","usa"),(1,"muni","usa")]

schema=StructType([StructField(name="id",dataType=IntegerType()),
                   StructField(name="Employee_name",dataType=StringType()),
                   StructField(name="location",dataType=StringType())
                   ])
df1=spark.createDataFrame(data,schema)   
display(df1)                

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

data=[("banana",1000,"usa"),("carrot",1500,"usa"),("beans",1600,"usa"),\
      ("orange",2000,"usa"),("orange",2000,"usa"),("banana",400,"china"),\
      ("carrot",1200,"china"),("beans",1500,"china"),("orange",4000,"china"),\
      ("banana",2000,"canada"),("carrot",2000,"canada"),("beans",2000,"mexico")
     ]
 
columns=['product','amount','country']

df=spark.createDataFrame(data,columns)
display(df)

# COMMAND ----------

df1=df.groupBy("product").pivot("country").sum("amount").show()



# COMMAND ----------


from pyspark.sql.functions import expr
df1.select("product",expr("stack(2,'canada',canada,'china',china) as (country,total)")).show()

# COMMAND ----------

from pyspark.sql.functions import expr

df1.select("product", expr("stack(2, 'canada', canada, 'china', china) as (country, total)")).show()

# COMMAND ----------

data=[("finance",10),("marketing",20),("sales",30),("it",40)]
schema=["dept_name","dept_id"]
df=spark.createDataFrame(data,schema)
display(df)    

# COMMAND ----------

from pyspark.sql.types import LongType
def addone(a):
    return a+1
addone_df=udf(addone,LongType())    

# COMMAND ----------

df.select("dept_name","dept_id",addone_df("dept_id") as (one)).show()

# COMMAND ----------

data=[("finance",10),("marketing",20),("sales",30),("it",40)]
schema=["dept_name","dept_id"]
df=spark.createDataFrame(data,schema)
display(df) 

# COMMAND ----------

from pyspark.sql.functions import upper
def uppername(df):
    return df.withColumn("dept_name",upper(df.dept_name))


# COMMAND ----------

df.transform(uppername).show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample

# COMMAND ----------

# MAGIC %sql
# MAGIC select dept_name from sample where dept_id in (10,20)

# COMMAND ----------

data=[(1,"chinna","india",10000),\
    (2,"ramu","uk",20000),\
        (3,"sreenu","india",30000),\
            (4,"sitha","uk",25000)\
                
              ]
schema=["id","name","country","salary"]   

df=spark.createDataFrame(data,schema)
display(df)          

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

window=Window.partitionBy("country").orderBy("salary")
df.withColumn("rank_column",F.rank().over(window)).show()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window

window=Window.partitionBy("county").orderBy(col("salary").desc())
df.withColumn("ran_column",F.rank().over(window)).show()

# COMMAND ----------

data=[("2023-01-25","2022-03-16 12:32:56.789"),
      ("2022-03-01","2022-03-16 01:23:45.678")]
df=spark.createDataFrame(data,["date_col","time_stamp"])      
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df.select("date_col",date_format("date_col","yyyy/MM/dd").alias("date")).show()

# COMMAND ----------

from pyspark.sql.functions import *
df.select("date_col",date_format("date_col","yyyy/MMMM/dd").alias("date")).show()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.select("date_col",date_sub("date_col",10).alias("modification_date"))

# COMMAND ----------

df.show()
df1.show()


# COMMAND ----------

df1.select("date_col","modification_date",datediff("date_col","modification_date")).show()

# COMMAND ----------

df1.select("date_col",year("date_col")).show()

# COMMAND ----------

df1.select("date_col",month("date_col")).show()

# COMMAND ----------

df1.select("date_col",dayofmonth("date_col")).show()

# COMMAND ----------

df1.select("date_col",dayofyear("date_col")).show()

# COMMAND ----------


