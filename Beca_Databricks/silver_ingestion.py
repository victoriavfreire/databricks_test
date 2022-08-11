# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark.sql.functions as F
import datetime, pytz
from pyspark.sql.functions import lit
from pyspark.sql import Window

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

name = 'victoria'
database = 'silver'
table = 'crime_'+name
location = f"/mnt/silver/crime-data_{name}"

# COMMAND ----------

df = spark.sql(f"""
                    select cast(complaintNumber as int) complaintNumber,
                            cast(keyCode as int) keyCode,
                            offenseDescription,
                            cast(policeDeptCode as int) policeDeptCode,
                            policeDeptDescription,
                            lawCategoryCode
                            jurisdictionDesc,
                            borough,
                            precinct,
                            locationOfOccurrenceDesc,
                            premiseTypeDesc,
                            latitude,
                            longitude,
                            reportDate
                    from bronze.crimes_{name}
                    where reportDate is not null
                    and complaintNumber is not null""")
        
display(df)

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

if not(DeltaTable.isDeltaTable(spark, location)):

    sql= "create database if not exists silver"
    spark.sql(sql)
    sql = "DROP TABLE IF EXISTS silver." + table + ";"
    spark.sql(sql)

    df.write.format("delta").mode("overwrite").save(location)

    sql = "CREATE TABLE silver." + table + " USING DELTA LOCATION '" + location + "'"
    spark.sql(sql)
    print(sql)
else:
    
    DeltaTable.forPath(spark, location).alias("base") \
    .merge(df.alias("newdata"), "base.complaintNumber=newdata.complaintNumber") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()    

