# Cloud Data Lake

Scripts to transform raw event and song data into parquet files on s3 using Python, AWS Redshift, and PostreSQL

## Table of contents

- General info
- Files Summary
- Technologies
- Setup
- Inspiratioin

### General Info
The format of this data lake allows the startup to easily query their data for analytical purposes, while saving storage space and the cost of keeping clusters continuously up and running. The design of the schema and tables is a star schema, which facilitates easy breakdown of the fact (in this case, when a song is played) across the dimensions of artist, song, user, and time. There is a table for the fact and each of these dimensions, with each table having unique Id's for each record (primary keys)

### Files Summary
The data sources for the staging tables come from two s3 buckets; Log data comes in the form of jsons from 's3://udacity-dend/log_data', and song data comes in csv's from 's3://udacity-dend/song_data'. The etl script uses pyspark sql's spark session to read json's from s3, transform them with user defined functions, and load them back into s3 as parquet files

dl.config holds the access key id and the secret access key from the IAM user created for our EMR cluster. etl.py contains all extract, load, and transform operations for the data pipeline

### Technologies (on EMR cluster)
Python 3.6.3
configparser 5.2.0
Spark 2.4.4
Hive 2.3.6

The technology we are using for our data lake is AWS Elastic MapReduce. EMR uses pyspark dataframes as well as Hive- SQL, it's query language, to process data

### Setup
To run these scripts, create an emr cluster in aws and copy etl.py to the notebook (JuptyerLab) connected to the cluster. Then open the terminal and run python3 etl.py

### Inspiration
This project created for the Udacity Data Engineering Nanodegree course, specifically the Data Lake unit

