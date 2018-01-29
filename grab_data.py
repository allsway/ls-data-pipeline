from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import boto3
import configparser
import pyspark
import sys
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq
from calculate_metrics import *
#from calculate_metrics_numpy import *
import global_settings 
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
from pyspark.sql.functions import lit

def get_key():
    return config.get('Params', 'aws_key')

def get_secret():
    return config.get('Params', 'secret_key')

def get_bucket():
	return config.get('Params', 'root_bucket')
	
def get_output_bucket():
	return config.get('Params', 'output_bucket')
	
def get_ip():
	return config.get('Dataconnector', 'ip')
	
def get_db():
	return config.get('Dataconnector', 'db_name')
	
def get_user():
	return config.get('Dataconnector', 'user')

def get_pass():
	return config.get('Dataconnector', 'password')

# save calculations to s3
def write_back_to_s3(df,key):
	print( global_settings.sc.textFile("s3://" + get_output_bucket()/ + "output/ " + key + ".csv"))
	s3_save_addr = "s3a://" + get_output_bucket() + "/output/" + key +  "_updated.csv"
	df.write.csv(s3_save_addr)

# connect to the data store
#def connect_to_datastore(df):
#	print(df.show())
#	conf = SparkConf()
#	conf.setMaster("172.31.16.59")
#	conf.setAppName("Spark Cassandra connector")
#	conf.set("spark.cassandra.connection.host","http://127.0.0.1")	
#	df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="adjacency_matrix", keyspace="livestories").save()


def connect_to_datastore(df):
	user = get_user()
	password = get_pass()
	db_name = get_db()
	test_query = '(SELECT * from test_table)'
	
	url = 'jdbc:postgresql://' + get_ip() + ':5432/' + db_name + '?user=' + user +'&password=' + password 
	#df = global_settings.sqlContext.read(url=url, table=test_query)
	
	spark = SparkSession.builder.appName('Write to Postgres').getOrCreate()
	
	postgres_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:" + db_name) \
        .option("dbtable", "metrics") \
        .option("user", user) \
        .option("password", password) \
        .load()

	print(postgres_df)
    # Saving data to a JDBC source
  #  jdbcDF.write \
   #     .format("jdbc") \
    #    .option("url", "jdbc:postgresql:" + db_name) \
     #   .option("dbtable", "metrics") \
    #    .option("user", "username") \
     #   .option("password", "password") \
   #     .save()
#	print(df.show())
	

def get_file_contents(file):
	print (file)
	if file['Key'] is not None:
		name = file['Key']
	indicator_id = name.split('.')[0]
	#filename = 's3a://' +get_bucket() +'/GOV.CA.CDPH.CDC.WONDER.OPIOID_1.L-D-I-V.idx.gz' 
	#filename = 's3a://' + get_bucket() + '*/*/*/*/*/*/*'
	filename = 's3a://' + get_bucket() +  '/' + name
	# wildcard 
	df = global_settings.sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(filename)
	print (df.show())
	
	distance_matrix, ref = calculate_distance(df,indicator_id)
	print (distance_matrix)
	return distance_matrix, ref;


def process_matrix(distance_matrix):
	#print(distance_matrix.take(5))
	#return_df = global_settings.sqlContext.createDataFrame(distance_matrix)
	#return_df = distance_matrix
	#return_df = return_df.withColumn('indicator_id',lit(indicator_id + '|' + ref))
	#return_df = return_df.withColumn('ref_location', lit(ref))
	return distance_matrix


# Handle files
def get_files(objects):
	list = objects['Contents']
	distance_matrix,ref = map(get_file_contents, list)
	return_df = map(process_matrix,distance_matrix)	

	#connect_to_datastore(list)
	#write_back_to_s3(return_df, name)	


# Connect to our S3 storage 
def connect_to_s3():
	conf = SparkConf().setAppName('text')
	global_settings.sc = pyspark.SparkContext()
	global_settings.sqlContext = SQLContext(global_settings.sc)

	#filename = 's3://ls-livedata-ds/digested/data/US.GOV.CDC.NNDSS.SURVVHEPA:CHRONB/observations/US.GOV.CDC.NNDSS.SURVVHEPA:CHRONB:0.D-I-L-V.idx.gz' 
	
	#df = global_settings.sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(filename)

	connection = S3Connection(get_key(), get_secret())
	bucket = connection.get_bucket(get_bucket())

	client = boto3.client('s3')
	bucket = get_bucket()
	
	objects = client.list_objects(Bucket = bucket)
	get_files(objects)
	

		
def calculate_distance(df,name):
	# Parallelize this tomorrow
	#new_df = df.toPandas()
	locale_class = 'US:ST'

	ref = 'US:ST:MN'
	#df.select('locale').map(lambda x: compare_locations(new_df, ref, locale_class))
	dists = compare_locations(df,ref,locale_class,global_settings.sc)
	print (dists)
	return dists, ref
	

config = configparser.ConfigParser()
config.read(sys.argv[1])
connect_to_s3()




