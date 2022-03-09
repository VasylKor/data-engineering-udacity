# AWS Data Lake Project

### Objective
A cloud based Data Lake is needed to process data relating to songs and user activity generated from a new music streaming app released by a startup. \
Data is extracted from JSON logs generated from user activity and JSON files about songs. JSON files are located in S3.\
In doing so, a pipeline is setup in order to extract the data from S3, process it and store it again in S3 as parquet files properly partitioned.

### ETL
Data is organised as a set of dimensional tables where **songplays** stores data about user activity collected from the logs. Other tables are: **users** (name, gender etc..), **songs** (title, artist etc..) and **artists**(name, location..). There's also a **"time"** table which enables to discriminate between different periods of time. \
The pipeline is built leveraging Spark and EMR from AWS. It takes as input the JSON documents relating logs and metadata, process it using the pyspark functions and returns parquet files appropriately partitioned.

### Repo
Here are present:
* `etl.py` : containg the spark instructions to be submitted;
* `data` : a folder with data used.
### Use
Set up an EMR cluster and a bucket for storing the output. The input is an S3.
In `etl.py` assign the S3 output path to the `output_data` variable in `main` function.
Copy the file into the EMR instance and submit it through the terminal/console.