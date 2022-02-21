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

<<PICTURE ARCHITECTURE>>

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

## Step 1: Scope the Project & Gather Data

## Step 2: Explore and Assess the Data

## Step 3: Define the Data Model

## Step 4: Run ETL to Model the Data

## Step 5: Complete Project Write Up
