# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import date,datetime
from pyspark.sql.window import Window
spark = SparkSession.builder.getOrCreate()
my_data = [(1,'Venky'),(2,'Naveen'),(3,'subbu'),(4,'ashok'),(5,'sathi')]
my_schema = ['id','Name'] 
df = spark.createDataFrame(data = my_data,schema= my_schema) 
df = df.withColumn('age',lit(26))
df = df.withColumn('split_column',upper(lit('arunachalam,tirumala,annavaram')))
#"""df = df.withColumn('split_column',explode(split(col('split_column'),','))).select('Name','split_column')"""
df = df.withColumn('Salary',when(col('id')== 1,10000).when(col('id') == 2,20000).when(col('id')==3,30000).when(col('id')==5,50000).otherwise(None))
df = df.withColumn('age',col('age').cast(IntegerType()))
df = df.withColumn('current_date',lit(current_date()))
df = df.withColumn('current_datetime',lit(current_timestamp()))
df = df.drop(col('current_datetime'))
new_df = [(6,'narayan',26,'',50000,date.today()),(7,'nasar',26,'',40000,date.today())]
df = df.union(spark.createDataFrame(new_df,schema= my_schema))
#window_spec = Window.partitionBy('id').orderBy(col('salary').desc())
window_spec = Window.orderBy(col('salary').desc())
#df = df.withColumn('rowNumber',row_number().over(window_spec))
#df = df.withColumn('rowNumber',rank().over(window_spec))
df = df.withColumn('rowNumber',dense_rank().over(window_spec))
df = df.withColumn('separator',locate(',',col('split_column')))
df = df.withColumn('length',length(col('Name')))
#df = df.withColumn('current_date',date_add(col('current_date'),-2))
#df = df.withColumn('current_date',add_months(col('current_date'),2))
df = df.withColumn('current_date',add_months(col('current_date'),12*2))
"""df = df.groupBy('salary').agg(count('*').alias('count_salarywise'),sum('salary').alias('sum_salary'),\
    min('salary').alias('min_salary'),max('salary').alias('max_salary'),avg('salary').alias('avg_salary'))"""
#df = df.filter(col('rowNumber') == 5)
#df = df.limit(3)
#print(df)
#df.show()

df.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('default.venkat_table')
    
df.write \
.format("delta") \
.mode("append") \
.saveAsTable("default.venkat_table")



# COMMAND ----------

#dbutils.fs.ls('/Workspace/Venkat')

# COMMAND ----------

#dbutils.widgets.text('Name','venkat')

# COMMAND ----------

"""var = dbutils.widgets.get('Name')
display(var)"""

# COMMAND ----------

# MAGIC %sql 
# MAGIC --CREATE DATABASE SALES

# COMMAND ----------

# MAGIC %sql 
# MAGIC /*CREATE TABLE SALES.MANAGETABLE(
# MAGIC   id bigint GENERATED ALWAYS AS IDENTITY,
# MAGIC   name varchar(100),
# MAGIC   age int
# MAGIC ) USING DELTA*/

# COMMAND ----------

# MAGIC %sql 
# MAGIC /*INSERT INTO SALES.MANAGETABLE(name,age) VALUES('VENKY',26),
# MAGIC ('SUBBU',28),
# MAGIC ('NAVEEN',26)*/

# COMMAND ----------

# MAGIC %sql
# MAGIC /*select * from sales.managetable

# COMMAND ----------

# MAGIC %sql 
# MAGIC --DROP TABLE IF EXISTS sales.managetable

# COMMAND ----------

#df2 = spark.read.table('default.venkat_table')
#display(df2)


# COMMAND ----------

"""from delta.tables import DeltaTable
deltaTable = DeltaTable.forName(spark,'default.venkat_table')
deltaTable.delete('salary is null')"""

# COMMAND ----------

#spark.catalog.dropTable('default.venkat_table')

# COMMAND ----------

"""from delta.tables import DeltaTable 
deltaTable = DeltaTable.forName(spark,'default.venkat_table')
deltaTable.delete()"""

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from default.venkat_table

# COMMAND ----------

"""from delta.tables import DeltaTable
scd_df1 = [('venkat',26,'DataEngineer',10000,True),
           ('subbu',28,'CivilEngineer',20000,False),
           ('ashok',29,'DataEngineer',30000,True),
           ('sathi',30,'DataEngineer',40000,False),
           ('naveen',31,'CivilEngineer',50000,True)]
scd_df = "name string,age int,designation string,salary int,isactive boolean"
scd_df1 = spark.createDataFrame(scd_df1,schema=scd_df)
#display(scd_df1)
delta_table1 = scd_df1.write.format('delta').mode('overwrite').saveAsTable('default.scd_delta_table1')
target_table1 = DeltaTable.forName(spark,"default.scd_delta_table1")
scd_df2 = [('venkat',26,'DataEngineer',10000,False)]
scd_df = "name string,age int,designation string,salary int,isactive boolean"
scd_df2 = spark.createDataFrame(scd_df2,schema=scd_df)
delta_table2 = scd_df2.write.format('delta').mode('overwrite').saveAsTable('default.scd_delta_table2')
target_table2 = DeltaTable.forName(spark,"default.scd_delta_table2")
#display(scd_df2)

target_table1.alias('Target').\
    merge(
        source = scd_df2.alias('source'),
        condition = 'Target.name = source.name'
    )\
    .whenMatchedUpdate(
        set = {
            'Target.isactive': 'source.isactive'
        }
    )\
    .whenNotMatchedInsert(
        values = {
            'name': 'source.name',
            'age': 'source.age',
            'designation': 'source.designation',
            'salary': 'source.salary',
            'isactive': 'source.isactive'
        }
    )\
    .execute()
display(target_table1.toDF())"""
           

# COMMAND ----------

"""from pyspark.sql.functions import *
from datetime import date,datetime
from delta.tables import DeltaTable
scd_df1 = [(1,'venkat',26,'DataEngineer',10000,True,date.today()),
           (2,'subbu',28,'CivilEngineer',20000,False,date.today()),
           (3,'ashok',29,'DataEngineer',30000,True,date.today()),
           (4,'sathi',30,'DataEngineer',40000,False,date.today()),
           (5,'naveen',31,'CivilEngineer',50000,True,date.today())]
scd_df = "id int,name string,age int,designation string,salary int,isactive boolean,current_date date"
scd_df1 = spark.createDataFrame(scd_df1,schema=scd_df)
#display(scd_df1)
delta_table1 = scd_df1.write.format('delta').mode('overwrite').saveAsTable('default.scd2_delta_table1')
target_table1 = DeltaTable.forName(spark,"default.scd2_delta_table1")
scd_df2 = [(1,'venkat',26,'DataEngineer',10000,False,date.today()),
           (4,'sathi',30,'CivilEngineer',40000,True,date.today())]
scd_df = "id int,name string,age int,designation string,salary int,isactive boolean,current_date date"
scd_df2 = spark.createDataFrame(scd_df2,schema=scd_df)
delta_table2 = scd_df2.write.format('delta').mode('overwrite').saveAsTable('default.scd2_delta_table2')
target_table2 = DeltaTable.forName(spark,"default.scd2_delta_table2")
target_table1.alias('Target').\
    merge(
        source = scd_df2.alias('source'),
        condition = 'Target.id = source.id and Target.isactive = True'
    )\
    .whenMatchedUpdate(
        set = {
            'Target.isactive': lit('false'),
            'Target.current_date' : current_date()
        }
    )\
    .execute()
target_table1.alias('Target').\
    merge(
        source = scd_df2.alias('source'),
        condition = 'Target.id = source.id and Target.isactive = True'
    )\
    .whenNotMatchedInsert(
        values = {
            'id': 'source.id',
            'name': 'source.name',
            'age': 'source.age',
            'designation': 'source.designation',
            'salary': 'source.salary',
            'isactive': 'source.isactive',
            'current_date': current_date()
        }
    )\
    .execute()
display(target_table1.toDF().orderBy(col('id').asc()))"""


# COMMAND ----------

# MAGIC %md
# MAGIC # ## Joins

# COMMAND ----------

"""from pyspark.sql.functions import *
df3 = [(1,'venkat',26,'DataEngineer'),\
    (2,'subbu',28,'CivilEngineer'),
    (3,'ashok',26,'Teacher')]
df4 = "id int,name string,age int,designation string"
df5 = spark.createDataFrame(df3,schema=df4)
#display(df5)
df6 = [(1,'hyderabad'),
       (3,'tirupathi')]
df7 = "id int,location string"
df8 = spark.createDataFrame(df6,schema=df7)
#display(df8)
df9 = df5.join(broadcast(df8),df5.id == df8.id,"inner")
display(df9)"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC --DESCRIBE HISTORY default.scd2_delta_table1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC --RESTORE TABLE default.scd2_delta_table1 TO VERSION AS OF 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM default.scd2_delta_table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --OPTIMIZE default.scd2_delta_table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM default.scd2_delta_table1;