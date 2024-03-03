#!/usr/bin/env python
# coding: utf-8



import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



parser = argparse.ArgumentParser()
 
 
parser.add_argument('--input_green')
parser.add_argument('--input_yellow')
parser.add_argument('--output')



args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

# Connecting to the local master cluster 

spark = SparkSession.builder\
        .appName("test")\
        .getOrCreate()

# print(f"green : {input_green} \n yellow : {input_yellow} \n output : {output} ")

df_green = spark.read.parquet(input_green)

df_yellow = spark.read.parquet(input_yellow)



df_green = df_green.withColumnRenamed("lpep_pickup_datetime","pickup_datetime")\
            . withColumnRenamed("lpep_dropoff_datetime","dropoff_datetime")



df_yellow = df_yellow.withColumnRenamed("tpep_pickup_datetime","pickup_datetime")\
                     .withColumnRenamed("tpep_dropoff_datetime","dropoff_datetime")


# Grouping two df's so selecting the common columns and grouping the data 


common_columns = []

yellow_unique_col = set(df_yellow.columns)

for i in df_green.columns:
    if i in yellow_unique_col:
        common_columns.append(i)


# Combining two df's
# 
# Using F.lit() for the 'word'


df_green = df_green.select(common_columns)\
        .withColumn('service_type',F.lit('green'))


# In[11]:


df_yellow = df_yellow.select(common_columns).withColumn("service_type",F.lit('yellow'))


# In[12]:


df_comb = df_green.unionAll(df_yellow)


# Temp Table

# In[13]:


df_comb.registerTempTable('trips_data')


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc("month", "pickup_datetime") AS revenue_month, 

    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

FROM 
    trips_data
GROUP by 1, 2, 3
""")


# df.write.coalase(<Number>).parquet() 
#     
# coalase means if we have partition then it will reduce the partition to the number of files 



df_result.write.parquet(output)






