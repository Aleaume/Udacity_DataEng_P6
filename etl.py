import configparser
from datetime import datetime
import calendar
from io import StringIO
import boto3
import os
import pandas as pd
import datetime
import psycopg2
epoch = datetime.datetime(1960, 1, 1)
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from pyspark.sql import types as T
from pyspark.sql import functions as F
from sql_queries import copy_table_queries, sql_counts



def create_spark_session():

    """
    Description: This function is responsible for initiating the Spark session with aws haddop package.
    Arguments:
    Returns:
        spark: the initiated spark session.
    """
        
    spark = SparkSession \
        .builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2") \
        .getOrCreate()
  
    return spark

def process_airports_data(s3_resource,s3_bucket):

    fname = 'airport-codes_csv.csv'
    df = pd.read_csv(fname)
    df_us = df.loc[(df['iso_country'] == "US")]
    df_us_airports = df_us[["iso_region","name","municipality","local_code","type","coordinates"]]
    df_us_airports['state_code'] = df_us_airports['iso_region'].str[3:5]
    df_us_airports["cityState"] = df_us_airports["state_code"]+df_us_airports["municipality"]
    df_us_airports['cityState'] = df_us_airports['cityState'].str.lower()
    df_us_airports.rename({'municipality': 'city'}, axis=1, inplace=True)
    df_us_airports = df_us_airports[["cityState","name","local_code","type","coordinates"]]
    
    csv_buffer = StringIO()
    df_us_airports.to_csv(csv_buffer,index=False)
    
    s3_resource.put_object(Bucket=s3_bucket, Key='P6/output/airports/airports.csv',Body=csv_buffer.getvalue())

    
def process_cities_data(s3_resource,s3_bucket):
    
    fname = 'us-cities-demographics.csv'
    df_cities = pd.read_csv(fname,sep=';')
    df_cities = df_cities.fillna(0)

    df_cities['Male Population'] = df_cities['Male Population'].astype(int)
    df_cities['Female Population'] = df_cities['Female Population'].astype(int)
    df_cities['Number of Veterans'] = df_cities['Number of Veterans'].astype(int)
    df_cities['Foreign-born'] = df_cities['Foreign-born'].astype(int)


    df_cities['cityState'] = df_cities['State Code']+df_cities['City']
    df_cities['cityState'] = df_cities['cityState'].str.lower()
    df_races =  df_cities[['cityState','Race','Count']]

    df_population = df_cities[['cityState','Median Age','Male Population','Female Population','Total Population','Number of Veterans','Foreign-born','Average Household Size']]
    df_population = df_population.drop_duplicates()

    df_cities = df_cities[['cityState','State Code','City','State']]
    df_cities = df_cities.drop_duplicates()
    
    # writing to S3 bucket
    csv_buffer = StringIO()
    df_cities.to_csv(csv_buffer, index=False)
    s3_resource.put_object(Bucket=s3_bucket, Key='P6/output/demographics/cities.csv',Body=csv_buffer.getvalue())
    
    csv_buffer = StringIO()
    df_races.to_csv(csv_buffer, index=False)
    s3_resource.put_object(Bucket=s3_bucket, Key='P6/output/demographics/races.csv',Body=csv_buffer.getvalue())
    
    csv_buffer = StringIO()
    df_population.to_csv(csv_buffer, index=False)
    s3_resource.put_object(Bucket=s3_bucket, Key='P6/output/demographics/population.csv',Body=csv_buffer.getvalue())
    

def process_weather_data(s3_resource,s3_bucket):
    
    fname = '../../data2/GlobalLandTemperaturesByCity.csv'
    df = pd.read_csv(fname)

    df_us = df.loc[(df['Country'] == "United States")]

    df_us['dt'] = pd.to_datetime(df_us['dt'],format='%Y-%m-%d')

    df_us['year'] = df_us['dt'].dt.year
    df_us['month'] = df_us['dt'].dt.month

    df_us = df_us.loc[(df_us['year'] > 1999)]

    df_weather = df_us[['City','AverageTemperature','year','month']]

    # writing to S3 bucket
    csv_buffer = StringIO()
    df_weather.to_csv(csv_buffer, index=False)
    s3_resource.put_object(Bucket=s3_bucket, Key='P6/output/weather/weather.csv',Body=csv_buffer.getvalue())
    
    
def process_immigration_data(spark,output_data,s3_resource,s3_bucket,month,year):
    
    fname = 'immigration_port.csv'
    df_immigration_port = pd.read_csv(fname)
    df_immigration_port['cityState'] = df_immigration_port['State_code']+df_immigration_port['City']
    df_immigration_port['cityState'] = df_immigration_port['cityState'].str.lower()
    df_immigration_port['City'] = df_immigration_port['City'].str.title()
    df_immigration_port = df_immigration_port[['cityState','City','State_code','country','immigration_code']]


    df_spark = spark.read.format('com.github.saurfang.sas.spark')\
                        .load('../../data/18-83510-I94-Data-{}/i94_{}{}_sub.sas7bdat'\
                        .format(year,(calendar.month_name[month]).lower(),year-2000))

    immigration_table = df_spark.select(["i94port","i94yr","i94mon","arrdate","depdate","i94mode","i94addr","biryear","gender","occup","airline","fltno","entdepa","entdepd","entdepu","matflag","visatype"])

    immigration_table = immigration_table.withColumnRenamed("i94port","entry_port") \
                            .withColumnRenamed("i94yr","year") \
                            .withColumnRenamed("i94mon","month") \
                            .withColumnRenamed("i94mode", "mode") \
                            .withColumnRenamed("biryear", "birthyear") \
                            .withColumnRenamed("occup", "occupation") \
                            .withColumnRenamed("fltno", "flightno") \
                            .withColumnRenamed("i94addr", "addr")
    
    convert_date = udf(lambda x: convert_sas(x),T.DateType())

    immigration_table = immigration_table.withColumn('arrdate',convert_date(immigration_table.arrdate))\
                                        .withColumn('depdate',convert_date(immigration_table.depdate))

    immigration_table = immigration_table.withColumn('year',col('year').cast('integer'))\
                    .withColumn('month',col('month').cast('integer'))\
                    .withColumn('mode',col('mode').cast('integer'))\
                    .withColumn('birthyear',col('birthyear').cast('integer'))
    
    # write immigration table to parquet files in s3
    immigration_table.write.option("header",True).mode("overwrite").parquet(output_data+"immigration/")
    
    
    # writing immigration port to S3 bucket
    csv_buffer = StringIO()
    df_immigration_port.to_csv(csv_buffer, index=False)
    s3_resource.put_object(Bucket=s3_bucket, Key='P6/output/immigration_port/immigration_port.csv',Body=csv_buffer.getvalue())
    
    
def convert_sas(sastime):
    try:
        return (pd.to_timedelta(sastime,unit='D')+ epoch)
    except:
        return None
    
def load_tables(cur, conn):
    
    """
        Description: This function is responsible for reading the json file in the s3 bucket, and copyting the data in the staging tables

        Arguments:
            cur: the cursor object.
            conn: xxx

        Returns:
            None
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def check_tables(cur, conn):
    
    """
        Description: This function is responsible for checking that each tables are properly filled by counting them.

        Arguments:
            cur: the cursor object.
            conn: xxx

        Returns:
            None
    """
    
    for query in sql_counts:
        cur.execute(query)
        result=cur.fetchone()
        print(query+" result is : "+str(result[0]))
        conn.commit()

        
def main():
    
    """
    Description: This main function is responsible for calling all functions previously defined in order.
                    This also where we can define the the input & output s3 locations.
    
    Arguments:
        None
        
    Returns:
        None
    """
    s3_bucket = "udacityexercisealeaume"
    
    year = 2016
    month = 5
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_AdminFull']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_AdminFull']['AWS_SECRET_ACCESS_KEY']

    
    s3_resource = boto3.client('s3')
    #spark = create_spark_session()
    
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    
    output_data = "s3a://udacityexercisealeaume/P6/output/"
    
    #process_airports_data(s3_resource,s3_bucket)
    #process_cities_data(s3_resource,s3_bucket)
    #process_weather_data(s3_resource,s3_bucket)
    #process_immigration_data(spark,output_data,s3_resource,s3_bucket,month,year)
    
    #load_tables(cur, conn)
    
    check_tables(cur, conn)
    
    
    conn.close()

if __name__ == "__main__":
    main()