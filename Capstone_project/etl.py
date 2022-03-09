import io
import os
import re
import configparser
from datetime import datetime, timedelta
import logging

import boto3

import pandas as pd

import pyspark
from pyspark.sql.functions import udf, col, to_timestamp, regexp_replace, round, lit, year, month, initcap, first
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession


# setup logs generator
logger = logging.getLogger()
logger.setLevel(logging.INFO)



# configuring AWS
config = configparser.ConfigParser()
config.read('fill_this.cnf', encoding='utf-8-sig')



AWS_S3_BUCKET = config['S3']['AWS_S3_BUCKET']
AWS_BUCKET_NAME = config['S3']['AWS_BUCKET_NAME']
AWS_ACCESS_KEY_ID = config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
S3_MAPPINGS_FOLDER = config['S3']['S3_MAPPINGS_FOLDER']
S3_PROCESSED_FOLDER = config['S3']['S3_PROCESSED_FOLDER']
S3_PRESENTATION_FOLDER = config['S3']['S3_PRESENTATION_FOLDER']
S3_RAW_FOLDER = config['S3']['S3_RAW_FOLDER']




spark = SparkSession.builder.\
config("spark.jars.repositories", "https://repos.spark-packages.org/").\
config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
enableHiveSupport().getOrCreate()




s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)



def sas_to_datetime(x):
    '''
    Gets datetime form sas' format
    '''
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        pass
sas_to_datetime_udf = udf(lambda x: sas_to_datetime(x), DateType())



def str_to_dt(x):
    '''
    Gets datetime from string
    '''
    try:
        return datetime.strptime(x, '%m%d%Y')
    except:
        pass
str_to_dt_udf = udf(lambda x: str_to_dt(x), DateType())




def export_as_csv_S3(df, filename, bucket):
    '''
    Exports df as csv in S3
    '''
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Bucket=bucket, Key=filename, Body=csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")






def process_labels(s3_client, bucket_name, raw_folder, mappings_folder):
    '''
    Processes the labels data containing the mappings
    for codes regarding our data.
    '''
    logging.info("Processing labels")

    fileobj = s3_client.get_object(
        Bucket=bucket_name,
        Key=f'{raw_folder}/I94_SAS_Labels_Descriptions.SAS'
        ) 

    filedata = fileobj['Body'].read()

    SAS_Label = filedata.decode('utf-8') 

    SAS_Label = ' '.join(SAS_Label.split())

    SAS_Label = SAS_Label.split('*')

    to_remove = []
    for parag in SAS_Label:
        if '=' in parag:
            pass
        else:
            to_remove.append(parag)

    for parag in to_remove:
        SAS_Label.remove(parag)

    cols_n_code = {}
    for parag in SAS_Label:
        if re.search(r'i94[\S]*',parag, re.IGNORECASE):
            stuff = []
            key = re.findall('i94[\S]*',parag, re.IGNORECASE)[0]
            value = re.findall(r"(('[\s\S]*?')|([0-9]+)?)[\s]?=[\s]?(('[\s\S]*?')|([a-zA-Z]*))",parag)
            for i in value:
                i = list(set(i))
                i = sorted(i, key=len)
                i.pop(0)
                stuff.append(i)
            cols_n_code[key] = stuff

    mapping_names = ['i94cntyl', 'i94prtl', 'i94model', 'i94addrl', 'I94VISA']
    index = 0
    for key in cols_n_code.keys():
        final_dict = {}
        n_0 = []
        n_1 = []
        for n in cols_n_code[key]:
            n_0.append(n[0])
            n_1.append(n[1])
        final_dict[f'{key}_0'] = n_0
        final_dict[f'{key}_1'] = n_1
        df = pd.DataFrame(final_dict)
        df = df.replace("'","",regex=True)
        export_as_csv_S3(df=df,
                        filename=f'{mappings_folder}/{mapping_names[index]}_mapping.csv',
                        bucket=bucket_name)
        index += 1





def process_immigration_data(spark, bucket ,raw_folder, mappings_folder, processed_folder):
    '''
    Processes data about immigration, renames columns,
    changes data types using udfs, joins with mapping tables
    to get eloquent names isnted of codes
    '''

    logging.info("Processing immigration data")

    immigr_df = spark.read.format('com.github.saurfang.sas.spark') \
                .load(f"{bucket}{raw_folder}/sas_data/*.sas7bdat")    

    i94addr_df = spark.read.format('csv') \
                .option("header","true") \
                .option('sep', ',') \
                .load(f'{bucket}{mappings_folder}/i94addrl_mapping.csv',)

    i94cnty_df = spark.read.format('csv') \
                .option("header","true") \
                .option('sep', ',') \
                .load(f'{bucket}{mappings_folder}/i94cntyl_mapping.csv',)

    i94model_df = spark.read.format('csv') \
                .option("header","true") \
                .option('sep', ',') \
                .load(f'{bucket}{mappings_folder}/i94model_mapping.csv',)

    i94prtl_df = spark.read.format('csv') \
                .option("header","true") \
                .option('sep', ',') \
                .load(f'{bucket}{mappings_folder}/i94prtl_mapping.csv',)
    i94prtl_df = i94prtl_df.withColumn('i94prtl_1', regexp_replace('i94prtl_1', r',[\s]?.*', ''))

    i94visa_df = spark.read.format('csv') \
                .option("header","true") \
                .option('sep', ',') \
                .load(f'{bucket}{mappings_folder}/I94VISA_mapping.csv',)

    immigr_df = immigr_df.withColumn("arrival_date", sas_to_datetime_udf("arrdate"))\
                        .withColumn("departure_date", sas_to_datetime_udf("depdate"))\
                        .withColumn("last_departure", str_to_dt_udf("dtaddto")) \
                        .drop("arrdate", "depdate", "dtaddto")

    immigr_df = immigr_df.withColumnRenamed('cicid', 'cic_id') \
                        .withColumnRenamed('i94yr', 'year') \
                        .withColumnRenamed('i94mon', 'month') \
                        .withColumnRenamed('i94port', 'city_code') \
                        .withColumnRenamed('i94addr', 'state_code') \
                        .withColumnRenamed('i94mode', 'transport') \
                        .withColumnRenamed('i94visa', 'visa') \
                        .withColumnRenamed('i94res', 'residence_country') \
                        .withColumnRenamed('i94cit', 'citizen_country') \
                        .withColumnRenamed('i94bir', 'age') \
                        .withColumnRenamed('biryear', 'birth_year') \
                        .withColumnRenamed('fltno', 'flight_number') \
                        .withColumnRenamed('admnum', 'admin_num') \
                        .withColumnRenamed('fltno', 'flight_number') 

    immigr_df = immigr_df.join(i94model_df,
                            [i94model_df.i94model_0 == immigr_df.transport],
                            'leftouter') \
                        .drop('transport') \
                        .drop('i94model_0') \
                        .withColumnRenamed('i94model_1','transport')

    immigr_df = immigr_df.join(i94addr_df,
                            [i94addr_df.i94addrl_0 == immigr_df.state_code],
                            'leftouter') \
                        .drop('i94addrl_0') \
                        .withColumnRenamed('i94addrl_1','state')

    immigr_df = immigr_df.join(i94cnty_df,
                            [i94cnty_df.i94cntyl_0 == immigr_df.citizen_country],
                            'leftouter') \
                        .drop('i94cntyl_0') \
                        .withColumnRenamed('citizen_country','citizen_country_code') \
                        .withColumnRenamed('i94cntyl_1','citizen_country')

    immigr_df = immigr_df.join(i94visa_df,
                            [i94visa_df.I94VISA_0 == immigr_df.visa],
                            'leftouter') \
                        .drop('I94VISA_0') \
                        .drop('visa') \
                        .withColumnRenamed('I94VISA_1','visa')

    immigr_df = immigr_df.join(i94cnty_df,
                            [i94cnty_df.i94cntyl_0 == immigr_df.residence_country],
                            'leftouter') \
                        .drop('i94cntyl_0') \
                        .withColumnRenamed('residence_country','residence_country_code') \
                        .withColumnRenamed('i94cntyl_1','residence_country')

    immigr_df = immigr_df.join(i94prtl_df,
                            [i94prtl_df.i94prtl_0 == immigr_df.city_code],
                            'leftouter') \
                        .drop('i94prtl_0') \
                        .withColumnRenamed('i94prtl_1','city')

    immigr_df.write.mode("overwrite").parquet(f"{bucket}{processed_folder}/immigration")


def process_temperature_data(spark, bucket ,raw_folder, processed_folder):
    '''
    Processes data about temperatures,
    get coordinates.
    '''

    logging.info("Processing temperature data")


    temp_df = spark.read.format('csv') \
            .option("header","true") \
            .load(f'{bucket}{raw_folder}/GlobalLandTemperaturesByCity.csv',)
    
    temp_df = temp_df.filter(temp_df.Country == 'United States')
    
    temp_df = temp_df.withColumn('Latitude', 
                             regexp_replace('Latitude',r'[a-zA-Z]*','').cast('double')) \
                 .withColumn('Longitude',
                             regexp_replace('Longitude',r'[a-zA-Z]*','').cast('double'))
    
    temp_df = temp_df.drop('Country')
    
    temp_df.write.mode("overwrite").parquet(f"{bucket}{processed_folder}/temperatures")



def process_demographics_data(spark, bucket ,raw_folder, processed_folder):
    '''
    Processes data about demographics. Among others,
    it drops duplicate rows caused by having a row per race category.
    It keeps the count of population by race making columns of them.
    '''    

    logging.info("Processing demographics data")


    demo_df = spark.read.format('csv') \
                .option("header","true") \
                .option('sep', ';') \
                .load(f'{bucket}{raw_folder}/us-cities-demographics.csv',)

    race_cols = demo_df.select(col('Race'),col('Count'),
                               col('State'),col('City'))

    race_cols = race_cols.groupby(race_cols.City, race_cols.State) \
        .pivot("Race") \
        .agg(first("Count"))

    demo_df = demo_df.drop('Race').drop('Count')

    demo_df = demo_df.drop_duplicates()

    cond = [demo_df.City == race_cols.City, demo_df.State == race_cols.State]

    demo_df = demo_df.join(race_cols, cond, 'inner') \
            .drop(race_cols.City) \
            .drop(race_cols.State)

    demo_df = demo_df.select(['City',
                 'State',
                 'Median Age',
                 'Male Population',
                 'Female Population',
                 'Total Population',
                 'Number of Veterans',
                 'Foreign-born',
                 'Average Household Size',
                 'State Code',
                 'American Indian and Alaska Native',
                 'Asian',
                 'Black or African-American',
                 'Hispanic or Latino',
                 'White'])

    demo_df = demo_df.withColumnRenamed('Median Age', 'median_age') \
    .withColumnRenamed('City', 'city') \
    .withColumnRenamed('State', 'state') \
    .withColumnRenamed('Male Population', 'male_population') \
    .withColumnRenamed('Female Population', 'female_population') \
    .withColumnRenamed('Total Population', 'total_population') \
    .withColumnRenamed('Number of Veterans', 'num_of_veterans') \
    .withColumnRenamed('Foreign-born', 'foreign_born') \
    .withColumnRenamed('Average Household Size', 'avg_household_size') \
    .withColumnRenamed('State Code', 'state_code') \
    .withColumnRenamed('American Indian and Alaska Native', 'american_indian_alaska_native') \
    .withColumnRenamed('Asian', 'asian') \
    .withColumnRenamed('Black or African-American', 'black_afroamerican') \
    .withColumnRenamed('Hispanic or Latino', 'hispanic_latino') \
    .withColumnRenamed('White', 'white') \
                     .selectExpr('City',
                                 'State',
                                 'male_population',
                                 'female_population',
                                 'cast(median_age as float) median_age',
                                 'total_population',
                                 'num_of_veterans',
                                 'foreign_born',
                                 'avg_household_size',
                                 'state_code',
                                 'american_indian_alaska_native',
                                 'asian',
                                 'black_afroamerican',
                                 'hispanic_latino',
                                 'white')

    demo_df.write.mode("overwrite").parquet(f"{bucket}{processed_folder}/demographics")


def process_airports_data(spark, bucket ,raw_folder, processed_folder):
    '''
    Processed airports data, getting coordinates
    '''

    logging.info("Processing airports data")


    airport_df = spark.read.format('csv') \
            .option("header","true") \
            .option('sep', ',') \
            .load(f'{bucket}{raw_folder}/airport-codes_csv.csv',)
    
    airport_df = airport_df.filter(col('iso_country') == 'US') \
    .withColumnRenamed('ident','id_local_code') \
    .withColumn('latitude', regexp_replace('coordinates', r'[\s]?.*,', '').cast('double')) \
    .withColumn('longitude', regexp_replace('coordinates', r',[\s]?.*', '').cast('double')) \
    .withColumn('state_code', regexp_replace('iso_region', r'[\s]?.*-', '')) \
    .drop('continent') \
    .drop('iso_country') \
    .drop('local_code') \
    .drop('gps_code') \
    .drop('coordinates')
    
    airport_df.write.mode("overwrite").parquet(f"{bucket}{processed_folder}/airports")


def process_presentation_data(spark,
                            bucket ,
                            processed_folder,
                            presentation_folder):
    
    '''
    Processes the processed data and 
    store it in S3 as the presentation layer
    '''

    logging.info("Importing data for presentation layer")
    
    demo_df = spark.read.parquet(f"{bucket}{processed_folder}/demographics")
    airport_df = spark.read.parquet(f"{bucket}{processed_folder}/airports")
    temps_df = spark.read.parquet(f"{bucket}{processed_folder}/temperatures")
    immi_df = spark.read.parquet(f"{bucket}{processed_folder}/immigration")

    '''Cities table'''

    logging.info("Processing cities data for presentation layer")

    city_coord = airport_df.groupBy(['municipality','state_code']) \
                        .agg({'latitude':'mean','longitude':'mean'}) \
                        .withColumnRenamed('avg(latitude)','latitude') \
                        .withColumnRenamed('avg(longitude)','longitude')

    city_cond = [demo_df.City==city_coord.municipality,
                demo_df.state_code==city_coord.state_code]

    city_df = demo_df.join(city_coord, city_cond, 'left')

    city_df = city_df.select(['City',
                            'State',
                            'latitude',
                            'longitude'])

    city_df.write \
    .partitionBy("State") \
    .mode("overwrite") \
    .parquet(f"{bucket}{presentation_folder}/cities")


    '''Airports per city'''

    logging.info("Processing airports data for presentation layer")

    airports = airport_df.join(demo_df,
                        [airport_df.state_code==demo_df.state_code,
                        airport_df.municipality==demo_df.City],
                        'outer') \
                        .select(['State',
                                'City',
                                'name',
                                'type',
                                'id_local_code',
                                'iata_code',
                                'elevation_ft',
                                'iso_region',
                                'latitude',
                                'longitude'])

    airports.write \
    .partitionBy(["State","City"]) \
    .mode("overwrite") \
    .parquet(f"{bucket}{presentation_folder}/airports")


    '''Temperatures'''
    '''
    Here we get temperatures by city. Since there are cities with same name
    but in different states, we join with coordinates taken from airport which
    actually have a state code. There is a certain error margin given
    by the fact that tha coordinates of the temperature table refer to the city
    while the airport's to the airports. 
    '''

    logging.info("Processing temperatures data for presentation layer")

    temps_df_buffer = temps_df.withColumnRenamed('City','city') \
                            .select(['dt','AverageTemperature','AverageTemperatureUncertainty','city',round('Latitude'),round('Longitude')]) \
                            .withColumnRenamed('round(Latitude, 0)','Latitude') \
                            .withColumnRenamed('round(Longitude, 0)','Longitude')

    city_df_buffer = city_df.select(['City','State',round('Latitude'),round('Longitude')]) \
                            .withColumnRenamed('round(Latitude, 0)','Latitude') \
                            .withColumnRenamed('round(Longitude, 0)','Longitude') \
                            .withColumn('Longitude', (col('Longitude')*-1)) \
                            .withColumn('Longitude_plus', (col('Longitude')+4)) \
                            .withColumn('Longitude_minus', (col('Longitude')-4)) \
                            .withColumn('Latitude_plus', (col('Latitude')+4)) \
                            .withColumn('Latitude_minus', (col('Latitude')-4))

    temps = temps_df_buffer.join(city_df_buffer,
                                [temps_df_buffer.city==city_df_buffer.City,
                                (temps_df_buffer.Latitude==city_df_buffer.Latitude) | (temps_df_buffer.Latitude<=city_df_buffer.Latitude_plus) | (temps_df_buffer.Latitude>=city_df_buffer.Latitude_minus),
                                (temps_df_buffer.Longitude==city_df_buffer.Longitude) | (temps_df_buffer.Longitude<=city_df_buffer.Longitude_plus) | (temps_df_buffer.Longitude>=city_df_buffer.Longitude_minus)],
                                'inner') \
                        .withColumnRenamed('dt','date') \
                        .withColumn('date', col('date').cast(DateType())) \
                        .withColumn('year', year(col('date'))) \
                        .withColumn('month', month(col('date'))) \
                        .drop('Latitude') \
                        .drop('Longitude') \
                        .drop('Longitude_plus') \
                        .drop('Longitude_minus') \
                        .drop('Latitude_plus') \
                        .drop('Latitude_minus') \
                        .drop(temps_df_buffer.city)

    temps = temps.select(['City',
                        'State',
                        'date',
                        'year',
                        'month',
                        'AverageTemperature',
                        'AverageTemperatureUncertainty'])

    temps.write \
    .partitionBy(["Year","State"]) \
    .mode("overwrite") \
    .parquet(f"{bucket}{presentation_folder}/temperatures")


    '''Demographics'''
    ''' Data relating demographics'''

    logging.info("Processing demographics data for presentation layer")

    demo_df.write \
    .partitionBy(["State","City"]) \
    .mode("overwrite") \
    .parquet(f"{bucket}{presentation_folder}/city_demographics")


    '''Immigration'''
    '''
    Data relating to the immigration phenomena
    '''

    logging.info("Processing immigration data for presentation layer")

    immigration = immi_df.select(['cic_id', 'year', 'month',
                                'city', 'state', 'airline',
                                'flight_number', 'arrival_date', 'departure_date',
                                'last_departure', 'transport', 'citizen_country',
                                'visa']) \
                        .withColumnRenamed('citizen_country','coming_from') \
                        .withColumnRenamed('visa','reason')
    
    immigration.write \
    .partitionBy(["state","year"]) \
    .mode("overwrite") \
    .parquet(f"{bucket}{presentation_folder}/immigration_info")


    '''Immigrants'''
    '''
    Data relating to the immigrants in particular
    '''

    logging.info("Processing immigrants data for presentation layer")


    immigrants =  immi_df.select(['cic_id','year','month',
                            'citizen_country','residence_country','city',
                            'state','age','birth_year','gender','visa']) \
                   .withColumnRenamed('visa','reason') \
                   .withColumnRenamed('state','state_of_arrival') \
                   .withColumnRenamed('city','city_of_arrival')         

    immigrants.write \
    .partitionBy(["state_of_arrival","residence_country"]) \
    .mode("overwrite") \
    .parquet(f"{bucket}{presentation_folder}/immigrants_info")


    '''Destination city demographics'''
    '''
    Data relating to the cities receiving immigration
    '''

    logging.info("Processing destination cities data for presentation layer")

    demo_buffer = demo_df.select(['City','State','total_population',
                              'median_age','avg_household_size',
                              'american_indian_alaska_native',
                              'asian','black_afroamerican',
                              'hispanic_latino','white'])

    immi_buffer = immi_df.select(['city','state','arrival_date','year','cic_id','citizen_country','residence_country','visa','age']) \
                        .withColumn('state', initcap(col('state'))) \
                        .withColumn('city', initcap(col('city')))

    destinations = immi_buffer.join(demo_buffer,
                            [immi_buffer.city==demo_buffer.City],
                            'left') \
                            .drop(immi_buffer.city) \
                            .drop(immi_buffer.state) \
                            .filter(col("City").isNotNull())

    destinations.write \
    .partitionBy(["residence_country","City"]) \
    .mode("overwrite") \
    .parquet(f"{bucket}{presentation_folder}/destinations_info")



process_labels(s3_client, AWS_BUCKET_NAME, S3_RAW_FOLDER, S3_MAPPINGS_FOLDER)
process_immigration_data(spark, AWS_S3_BUCKET,S3_RAW_FOLDER, S3_MAPPINGS_FOLDER, S3_PROCESSED_FOLDER)
process_temperature_data(spark, AWS_S3_BUCKET,S3_RAW_FOLDER, S3_PROCESSED_FOLDER)
process_demographics_data(spark, AWS_S3_BUCKET,S3_RAW_FOLDER, S3_PROCESSED_FOLDER)
process_airports_data(spark, AWS_S3_BUCKET,S3_RAW_FOLDER, S3_PROCESSED_FOLDER)
process_presentation_data(spark, AWS_S3_BUCKET, S3_PROCESSED_FOLDER, S3_PRESENTATION_FOLDER)

def check(spark, bucket, presentation_folder, table):
    df = spark.read.parquet(f"{bucket}{presentation_folder}/{table}")
    if len(df.columns) > 0 and df.count() > 0:
        pass
    else: 
        return f"{table} didn't pass the check"


tables = ['cities','airports',
        'temperatures','city_demographics',
        'immigration_info','immigrants_info',
        'destinations_info']

errors = []
for table in tables: 
    res = check(spark, AWS_S3_BUCKET,S3_PRESENTATION_FOLDER, table)
    if res:
        errors.append(res)

if errors:
    raise Exception(f'''
    Data has been processed but
    the followings have not passed data
    quality check:
    {errors}''')
