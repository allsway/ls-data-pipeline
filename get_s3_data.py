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
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
#import spark-s3

#import pyspark_cassandra


def get_key():
    return config.get('Params', 'aws_key')

def get_secret():
    return config.get('Params', 'secret_key')

def get_bucket():
	return config.get('Params', 'root_bucket')
	
def get_output_bucket():
	return config.get('Params', 'output_bucket')
	
def get_ips():
	return config.get('Dataconnector', 'ips')
	
def get_schema():
	return config.get('Dataconnector', 'schema')



#def connect_to_datastore():
	#config = new HBaseConfiguration()
    #hbaseContext = new HBaseContext(sc, config)

# save calculations to s3
def write_back_to_s3(df,key):
	s3_save_addr = "s3a://" + get_output_bucket() + "/output/" + key +  "_updated.csv"
	df.write.csv(s3_save_addr)

# connect to the data store
def connect_to_datastore(df):
	conf = SparkConf()
	conf.setMaster("172.31.16.59")
	conf.setAppName("Spark Cassandra connector")
	conf.set("spark.cassandra.connection.host","http://127.0.0.1")
	#sc = sc("spark://172-31-16-59:7077", "test", conf)
	
	df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="single_column", keyspace="livestories").save()


# Handle files
def get_files(objects, sqlContext):
	for file in objects['Contents']:
		name = file['Key']
		print (file['Key'])
		filename = 's3a://' + get_bucket() +  '/' + name
		df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(filename)
		print (df.show())
		
		distance_matrix = calculate_distance(df,name)
		return_df = sqlContext.createDataFrame(distance_matrix)
		connect_to_datastore(return_df)
		write_back_to_s3(return_df, file['Key'])	


# Connect to our S3 storage 
def connect_to_s3():

	connection = S3Connection(get_key(), get_secret())
	bucket = connection.get_bucket(get_bucket())

	client = boto3.client('s3')
	bucket = get_bucket()
	
	objects = client.list_objects(Bucket = bucket)
	conf = SparkConf().setAppName('text')
	sc = pyspark.SparkContext()
	sqlContext = SQLContext(sc)
	get_files(objects,sqlContext)
	

		
def calculate_distance(df,name):
	# Parallelize this tomorrow
	indicator_id = name.split('.')[0]
	new_df = df.toPandas()
	locale_class = 'US:ST'

	ref = 'US:ST:MN'
	#df.select('locale').map(lambda x: compare_locations(new_df, ref, locale_class))
	dists = compare_locations(new_df,ref,locale_class)
	return dists
	

config = configparser.ConfigParser()
config.read(sys.argv[1])
connect_to_s3()




