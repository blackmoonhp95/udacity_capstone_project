from pyspark.sql import SparkSession
from variables import *

spark = SparkSession.builder.appName("Data quality check").getOrCreate()

# 1. Data schema of every dimensional table matches data model
for path in list_target_paths:
    table_name = path.split("/")[1].replace("_path", "") 
    df = spark.read.parquet(path)
    print("Table: {}".format(table_name))
    schema = df.printSchema()
    
# 2. No empty table after running ETL data pipeline
for path in list_target_paths:
    table_name = path.split("/")[1].replace("_path", "")
    df = spark.read.parquet(path)
    if df.count() <= 0:
        raise ValueError("This table is empty!")
    else:
        print("Table {} had data".format(table_name))
    
