# Udacity_DataEng_P6
Udacity Course - Data Engineering - Data Engineering Capstone Project

## Requirements
The following tools & libraries are necessary to this project
- Apache Spark
- AWS S3 Bucket
- AWS Redshift
- Following libraries : configparser, boto3, psycopg2, ...

## Overview
In this project the goal is to scope datasets, explore, assess, define a model and then finally create a surrounding ETL pipeline for this data.
I used for my project the porposed dataset from Udacity course.
In my taking of the dataset, I decided to pick up the data from the Udacity workspace, clean & transform it to my needs, then push it to my S3 Bucket.
Once in AWS, the data is then copied over to a Redshift DWH to be ready to be consumed by any data analyst.

### Architecture

The project is composed of different cloud components and a few scripts working together as described in this diagram:

< PICTURE ARCHITECTURE >

#### The S3 Bucket
  
The S3 bucket is created for the purpose of this project in my AWS instance.

#### Jupyter Notebooks
  
There are 2 jupyter notebookes in this project.
  
- Capstone Project.ipynb is based on the template given by Udacity, where the different steps of the project are detailed
- redshift_create.ipynb is a ready-made script I previously developped in P3 of this course to automate and easily deploy all the needed clients / components for an up running Redshift Datawarehouse

#### Python Scripts
  
- etl.py is the main script to use in order to extract, transform, push to s3 and copy data to redhsift.
- create_tables.py is the script used to prepare the Redshift DB with the needed tables.
- sql_queries.py consumed by the other 2 python scripts is a simple holder of all SQL queries needed in this project.
  
#### Config file
  
The file dl.cfg, holds all information needed to reach AWS, authenticate but also to create the Redshift cluster with all configurations needed.

#### Redshift Cluster

The Data warehouse used in this exercise is an AWS Redshift cluster. It is actually configured and set up via jupyter notebook (see section above).
In this example we made used of the following parameters:

Param | Value 
--- | --- 
DWH_CLUSTER_TYPE	| multi-node
DWH_NUM_NODES	| 4
DWH_NODE_TYPE	| dc2.large
DWH_CLUSTER_IDENTIFIER	| dwhCluster



## Step 1: Scope the Project & Gather Data

### Scope
The scope of this project is readily compile and join those disparate datasets in a functional single point of reference in order for data analyst and business users to be able to query it easily and efficiently. Scenarios such as targeted advertisement for foreign-tourist in the US could for example benefiting from the followin tool.
Question such as "What port/city do immigrant get in from the most ?", or What are the top 10 landing airports for B2 visa holders ?
  
 In order to do so, the objective is to load all the below specified datasets in a central AWS Redshift DB. For that we first extract the data from the Udacity Workspace. Transfom it to our need, and then load it to tailored S3 buckets. Once done, we only need to load it to a Redshift DB.
  

### Dataset
There are 4 Datasets picked up from the Udacity ones and 1 created from info:
  
#### I94 Immigration Data
This data comes from the US National Tourism and Trade Office.
It depicts the alien arrivals & departures data collected.

#### World Temperature Data
This is a simple dataset with daily average temerature by city.

#### U.S. City Demographic Data
This data details the demographics in the US by city, including ethnicity, veterans count and foreign-borns.
  
#### Airport Code Table
This simple dataset, matches up airport codes & names with the cities there are located at.
  
#### immigration port
This simple dataset I have created taken fron the Description file of the I94 Data, is helping to map out efficiently the I94port fields to the state and city corresponding

## Step 2: Explore and Assess the Data

This step is detailed in the "Capstone Project.ipynb" document.

## Step 3: Define the Data Model

### Conceptual Data Model

In order to offer the best flexibility to data consumers and data analysts, we need to set out a performant model capable to reach the table needed in maximum 3 joins.
With this in mind I organozed data in the Redshift DB in a snowflake schema made of Fact and dimesion tables as detailed in the diagram below:
  
 < PICTURE OF DIAGRAM >
 
### Mapping out Data Pipelines

In order to bring the above model to life, we need to go through the followin steps:

1. First we need to make sure we have an up and running Redshift Cluster with TCP open on port 5439.
As described previously we accomplish this thanks to a jupyter notebook.

2. Then we need to make sure to create the tables needed.
For this, we created a series of sql queries for each table defining its structure, in the file sql_queries.py

```python

# CREATE TABLES

airport_table_create= ("""CREATE TABLE IF NOT EXISTS airport(\
                                cityState varchar,
                                name varchar,
                                local_code varchar,
                                type varchar,
                                coordinates varchar
    )

""")

cities_table_create = ("""CREATE TABLE IF NOT EXISTS cities(\
                    cityState varchar,\
                    state_code varchar,\
                    city varchar,\
                    state_name varchar);

""")

population_table_create = ("""CREATE TABLE IF NOT EXISTS population(\
                        cityState varchar, \
                        median_age float, \
                        male_population int,\
                        female_population int,\
                        total_population int,\
                        number_veterans int,\
                        foreign_born int,\
                        avg_household_size float);
""")

race_table_create = ("""CREATE TABLE IF NOT EXISTS race(\
                    cityState varchar,\
                    race varchar,\
                    count int);
""")

immigration_table_create = ("""CREATE TABLE IF NOT EXISTS immigration(\
                    entry_port varchar,\
                    year int,\
                    month int,\
                    arrdate timestamp,\
                    depdate timestamp,\
                    mode float,\
                    addr varchar,\
                    birthyear int,\
                    gender varchar,\
                    occupation varchar,\
                    airline varchar,\
                    flightno varchar,\
                    entdepa varchar,\
                    entdepd varchar,\
                    entdepu varchar,\
                    matflag varchar,\
                    visatype varchar);
""")

immigration_port_table_create = ("""CREATE TABLE IF NOT EXISTS immigration_port(\
                        cityState varchar,\
                        country varchar,\
                        immigration_code varchar);
""")

weather_table_create = ("""CREATE TABLE IF NOT EXISTS weather (\
                    city varchar,\
                    avgTemp float,\
                    year int,\
                    month int);
""")

```

We then simply execute the script create_tables.py and can see in Redshift the empty tables beign created.



NOTE: for the purpose of the exercise, the create_tables script also drops the tables before creating them. Making it easy for the development / implementation phase.


```python

airport_table_drop = "DROP table IF EXISTS airport"
cities_table_drop = "DROP table IF EXISTS cities"
population_table_drop = "DROP table IF EXISTS population"
race_table_drop = "DROP table IF EXISTS race"
immigration_table_drop = "DROP table IF EXISTS immigration"
immigration_port_table_drop = "DROP table IF EXISTS immigration_port"
weather_table_drop = "DROP table IF EXISTS weather"

```
3. Next step is to then extract the data
In this scenario we assume data is stored in the Workspace of Udacity. Depending on the file type we either load the data in a spark dataframe for SAS7BDAT files or load the csv files in respective pandas dataframes.

```python 
fname = 'airport-codes_csv.csv'
    df = pd.read_csv(fname)
    
```

```pthon

df_spark = spark.read.format('com.github.saurfang.sas.spark')\
                        .load('../../data/18-83510-I94-Data-{}/i94_{}{}_sub.sas7bdat'\
                        .format(year,lower(calendar.month_name[month]),time.strftime("%y",year)))

```

NOTE: in order to pick up SAS7BDAT files with Spark we need laod the appropriate library, in our ETL pipeline, we define a purposefull function dedicated to create the spark session with needed libraries and return it.

```python

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

```

4. Step for if the central step were data cleaning & wrangling is done.


## Step 4: Run ETL to Model the Data

## Step 5: Complete Project Write Up
