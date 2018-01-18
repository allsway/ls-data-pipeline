from azure.storage.blob import BlobService
from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import configparser
import pyspark
import sys
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.mllib.stat import Statistics
import numpy


def get_key():
    return config.get('Params', 'aws_key')

def get_secret():
    return config.get('Params', 'secret_key')

def get_bucket():
	return config.get('Params', 'root_bucket')

#livestories = sc.textFile("wasbs:///ams/BRFSS500_37.csv")

#print(livestories.count())

#def connect_to_datastore():
	#config = new HBaseConfiguration()
    #hbaseContext = new HBaseContext(sc, config)


# Connect to our S3 storage 
def connect_to_s3():
	connection = S3Connection(get_key(), get_secret())
	bucket = connection.get_bucket(get_bucket())
	sc = pyspark.SparkContext(appName="Test")

	# Filter by new data? 
	for file in bucket:
		# Transform to HDFS table
		rdd = sc.parallelize(file,20)
		# 
		indicator = rdd.map(lambda x: Row(id=x[1], locale=x[2], date_range=x[3], dimension=x[4], type_of_locale=x[5], category=x[6]))
		sqlContext = SQLContext(sc)
		schemaIndicator = sqlContext.createDataFrame(indicator)
		calculate_distance(schemaIndicator,sqlContext)
		
		
def calculate_distance(df, sqlContext):

	
	locales = df.select(df("locale")).distinct
	print (locales)
	
	average = avg(df["locale"])
	#zscores = udf(lambda row: (x - average) / standard_dev)

#	print (zscores)

#	u = df.groupBy("locale").agg(df.date_range, avg("locale"), stddev_pop("locale"))		
	


config = configparser.ConfigParser()
config.read(sys.argv[1])
connect_to_s3()




