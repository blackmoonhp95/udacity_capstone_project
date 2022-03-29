import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date
from pyspark.sql.functions import monotonically_increasing_id
from variables import * 


# data processing functions
def create_spark_session():
    '''
    Create SparkSession object
    '''
    spark = SparkSession.builder\
                    .appName("Capstone Project")\
                    .getOrCreate()
    return spark


def SAS_to_date(date):
    '''
    Convert SAS to date
    '''
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')

SAS_to_date_udf = udf(SAS_to_date, DateType())


def rename_columns(table, new_columns):
    '''
    Mapping new column names
    '''
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)

    return table

def process_immigration_data(spark, input_data, *output_data):
    """
    Process immigration data to get 
        fact_immigration, 
        dim_immi_personal,
        dim_immi_airline tables
    Arguments:
        spark {object}: SparkSession object
        input_data {object}: Source data path
        output_data {object}: Target data path
    """
    
    # read immigration data file
    df = spark.read.parquet("sas_data")

    # extract columns to create fact_immigration table
    fact_immigration_df = df.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr',\
                                 'arrdate', 'depdate', 'i94mode', 'i94visa').distinct()\
                         .withColumn("immigration_id", monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cic_id', 'year', 'month', 'city_code', 'state_code',\
                   'arrive_date', 'departure_date', 'mode', 'visa']
    
    fact_immigration_df = rename_columns(fact_immigration_df, new_columns)

    fact_immigration_df = fact_immigration_df.withColumn('country', lit('United States'))\
                        .withColumn('arrive_date', SAS_to_date_udf(col('arrive_date')))\
                        .withColumn('departure_date', SAS_to_date_udf(col('departure_date')))

    # write fact_immigration table to parquet files partitioned by state and city
    fact_immigration_df.write.partitionBy("state_code")\
                    .parquet(output_data[0], mode="overwrite")
    
    # extract columns to create dim_immi_personal table
    dim_immi_personal_df = df.select('cicid', 'i94cit', 'i94res',\
                                  'biryear', 'gender', 'insnum').distinct()\
                          .withColumn("immi_personal_id", monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cic_id', 'citizen_country', 'residence_country',\
                   'birth_year', 'gender', 'ins_num']
    dim_immi_personal_df = rename_columns(dim_immi_personal_df, new_columns)

    # write dim_immi_personal table to parquet files
    dim_immi_personal_df.write.parquet(output_data[1], mode="overwrite")

    # extract columns to create dim_immi_airline table
    dim_immi_airline_df = df.select('cicid', 'airline', 'admnum', 'fltno', 'visatype').distinct()\
                         .withColumn("immi_airline_id", monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cic_id', 'airline', 'admin_num', 'flight_number', 'visa_type']
    dim_immi_airline_df = rename_columns(dim_immi_airline_df, new_columns)

    # write dim_immi_airline table to parquet files
    dim_immi_airline_df.write.parquet(output_data[2], mode="overwrite")
    
    
def process_temperature_data(spark, input_data, *output_data):
    """
    Process temperature data to get dim_temperature table
    Arguments:
        spark {object}: SparkSession object
        input_data {object}: Source data path
        output_data {object}: Target data path
    """

    # read temperature data file
    df = spark.read.csv(input_data, header=True)

    df = df.where(df['Country'] == 'United States')
    dim_temperature_df = df.select(['dt', 
                                    'AverageTemperature', 
                                    'AverageTemperatureUncertainty', 
                                    'City', 
                                    'Country']).distinct()

    new_columns = ['dt', 'avg_temp', 'avg_temp_uncertnty', 'city', 'country']
    dim_temperature_df = rename_columns(dim_temperature_df, new_columns)

    dim_temperature_df = dim_temperature_df.withColumn('dt', to_date(col('dt')))
    dim_temperature_df = dim_temperature_df.withColumn('year', year(dim_temperature_df['dt']))
    dim_temperature_df = dim_temperature_df.withColumn('month', month(dim_temperature_df['dt']))
 
    # write dim_temperature table to parquet files
    dim_temperature_df.write.mode("overwrite")\
                   .parquet(path=output_data[0])


def process_graphic_data(spark, input_data, *output_data):
    """ 
    Process graphic data to get 
        dim_graphic_population 
        dim_graphic_statistics table
    Arguments:
        spark {object}: SparkSession object
        input_data {object}: Source data path
        output_data {object}: Target data path
    """
    # read graphic data file
    df = spark.read.options(header=True, delimiter=';').csv(input_data)


    dim_graphic_population_df = df.select(['City', 'State', 'Male Population', 'Female Population', \
                              'Number of Veterans', 'Foreign-born', 'Race']).distinct() \
                              .withColumn("graphic_pop_id", monotonically_increasing_id())


    new_columns = ['city', 'state', 'male_population', 'female_population', \
                   'num_vetarans', 'foreign_born', 'race']
    dim_graphic_population_df = rename_columns(dim_graphic_population_df, new_columns)

    # write dim_graphic_population table to parquet files
    dim_graphic_population_df.write.parquet(output_data[0], mode="overwrite")

    
    dim_graphic_statistics_df = df.select(['City', 'State', 'Median Age', 'Average Household Size'])\
                             .distinct()\
                             .withColumn("graphic_stat_id", monotonically_increasing_id())

    new_columns = ['city', 'state', 'median_age', 'avg_household_size']
    dim_graphic_statistics_df = rename_columns(dim_graphic_statistics_df, new_columns)
    dim_graphic_statistics_df = dim_graphic_statistics_df.withColumn('city', upper(col('city')))\
                                                    .withColumn('state', upper(col('state')))

    # write dim_graphic_statistics table to parquet files
    dim_graphic_statistics_df.write.parquet(output_data[1], mode="overwrite")

    

def main():
    spark = create_spark_session()
    
    process_immigration_data(spark, 
                             immigration_data_path, 
                             fact_immigration_path, 
                             dim_immi_personal_path, 
                             dim_immi_airline_path)
    
    process_temperature_data(spark, 
                             temperature_data_path, 
                             dim_temperature_path)
    
    process_graphic_data(spark, 
                         graphic_data_path, 
                         dim_graphic_population_path, 
                         dim_graphic_statistics_path)
    

if __name__ == "__main__":
    main()