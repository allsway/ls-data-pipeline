from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import boto3
import configparser
import pyspark
import sys
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq
#from calculate_metrics import *
#from calculate_metrics_numpy import *
#from dataframe_metrics import *
import global_settings 
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
from pyspark.sql.functions import lit
import pyspark.sql.functions as func
from pyspark.sql.types import *
import numpy as np
import pandas as pd
from scipy import spatial
from scipy import stats
import math
import psycopg2

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
	
#def get_db():
#	return config.get('Dataconnector', 'db_name')
	
def get_user():
	return config.get('Dataconnector', 'user')

def get_pass():
	return config.get('Dataconnector', 'password')
	
def get_connection():
	return config.get('Dataconnector','data_connection')

# save calculations to s3
def write_back_to_s3(df,key):
	print( global_settings.sc.textFile("s3://" + get_output_bucket()/ + "output/ " + key + ".csv"))
	s3_save_addr = "s3a://" + get_output_bucket() + "/output/" + key +  "_updated.csv"
	df.write.csv(s3_save_addr)


def get_file_contents(file):
	print (file)
	if file['Key'] is not None:
		name = file['Key']
	indicator_id = name.split('.')[0]
	print(indicator_id)
	filename = 's3a://' + get_bucket() +  '/' + name
	
	# read in data without headers from S3
	schema = StructType([
		StructField("indicator_id", StringType(), False),
		StructField("locale", StringType(), False),
		StructField("interval", StringType(), False),
		StructField("dimension_id", StringType(), False),
		StructField("value", FloatType(), False)
	])

	# wildcard 
	dataframe = global_settings.sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(filename)
	print (dataframe.show())
	distance_matrix = compare_locations(dataframe,indicator_id)
	print (distance_matrix)


# Handle files
def get_files(objects):
	list = objects['Contents']
	print(list)
	distance_matrix = map(get_file_contents, list)
	#connect_to_datastore(list)
	#write_back_to_s3(return_df, name)	


# Connect to our S3 storage 
def connect_to_s3():
	conf = SparkConf().setAppName('text')
	global_settings.sc = pyspark.SparkContext()
	global_settings.sqlContext = SQLContext(global_settings.sc)
	connection = S3Connection(get_key(), get_secret())
	bucket = connection.get_bucket(get_bucket())

	client = boto3.client('s3')
	bucket = get_bucket()
	print(bucket)
	objects = client.list_objects(Bucket = bucket)
	get_files(objects)



# Returns a Dataframe with the zscore calculated based on each dimension 
def get_zscore_comparison(df):
	# Get the mean and stddev for each dimension
	mean_df = df.groupBy(df.dimension_id,df.interval).agg(avg('value'))
	sdev = df.groupBy(df.dimension_id,df.interval).agg(stddev('value'))
	
	df = df.join(mean_df, ['interval', 'dimension_id'], 'inner')
	df = df.join(sdev, ['interval', 'dimension_id'], 'inner')
	df = df.withColumn('stddev', df["stddev_samp(value)"].cast(FloatType()))
	df = df.withColumn('avg', df["avg(value)"].cast(FloatType()))

	df = df.rdd.map(lambda row: ( row.interval , row.dimension_id, 
		row.locale, row.value, row.locale_class, 
		row.value if (math.isnan(float(row.stddev)) or float(row.stddev) == 0) else (row.value - row.avg) / row.stddev ))

	schema = StructType([
		StructField("interval", StringType(), False),
		StructField("dimension_id", StringType(), False),
		StructField("locale", StringType(), False),
		StructField("value", FloatType(), False),
		StructField("locale_class", StringType(), False),
		StructField("zscore", FloatType(), False)])

	returned_df = global_settings.sqlContext.createDataFrame(df, schema)
	print(returned_df.show())
	return returned_df


# Test for inserting row by row into postgres
def insert_into_postgres(row, cursor):
    query =  "INSERT INTO distance_table (locale,comp_locale,distance,interval) VALUES (%s, %s, %d, %s);"
    data = (row[0], row[1], row[2], row[3])
    cursor.execute(query, data)
    cursor.commit()
    return row
    
	
# Saves the distances, correlations and weighted correlations to postgres
def write_to_db(df,table):
	print(df.show())
	user_config = get_user()
	password_config = get_pass()
	#url = 'jdbc:postgresql://' + get_ip() + ':5432/' + db_name + '?user=' + user_config +'&password=' + password_config 

	df.write \
    .format("jdbc") \
    .mode('append') \
    .option("driver", "org.postgresql.Driver") \
    .option("url", get_connection()) \
    .option("dbtable", table) \
    .option("user", get_user()) \
    .option("password", get_pass()) \
    .save()


# Reduces set of dimensions to the ones that are the same between the two locations
def compare_dimensions(row):
	ref_dictionary = dict(zip(row[4], row[3]))
	comp_dictionary = dict(zip(row[7], row[6]))
	if(set(row[4]) != set(row[7])):
		ref_keys = set(ref_dictionary.keys())
		comp_keys = set(comp_dictionary.keys())
		for item in ref_dictionary.keys():
  			if not comp_dictionary.has_key(item):
  				del ref_dictionary[item]
  		for item in comp_dictionary.keys():
  			if not ref_dictionary.has_key(item):
  				del comp_dictionary[item]
	
	distance = float('nan')
	distance = spatial.distance.euclidean(ref_dictionary.values(), comp_dictionary.values())
	return row[2], row[5], distance, row[0]
	


# Calculate the eudclidean distance between all locations in the file
def get_euclidean_distances(new_df):
	# Groups each row by location and interval, and gets n-dimensional vector of values for each (loc, interval) 
	distance_rows = new_df.select('locale','interval','dimension_id','zscore','locale_class').groupBy(new_df.locale,
		new_df.interval,new_df.locale_class).agg(func.collect_list('zscore'),
		func.collect_list('dimension_id')).withColumnRenamed("collect_list(zscore)", "ref_zscores").withColumnRenamed("collect_list(dimension_id)", "ref_dimensions")

	# Join the dataframe to itself on interval, resulting in a location x location table
	distance_rows = distance_rows.join(distance_rows.select('interval', 'locale_class',
		col('locale').alias('comp_locale'),
		col('ref_zscores').alias('comp_zscores'),
		col('ref_dimensions').alias('comp_dimensions')), ['interval','locale_class']) #.sort(col('locale').desc())
	
	print(distance_rows.show())
	schema = StructType([
		StructField("locale", StringType(), False),
		StructField("locale2", StringType(), False),
		StructField("interval", StringType(), False),
		StructField("distance", FloatType(), False)])
		
	print('distance test:\n')
	print(distance_rows.rdd.getNumPartitions())

	checked_distances = distance_rows.rdd.map(compare_dimensions)
	print('Distances after verification')
	print(checked_distances.take(5))
	
	schema = StructType([
		StructField("locale", StringType(), False),
		StructField("comp_locale", StringType(), False),
		StructField("distance", FloatType(), False),
		StructField("interval", StringType(), False),

		])

	distance_df = global_settings.sqlContext.createDataFrame(checked_distances, schema)
	print('Distance dataframe:')
	print(distance_df.show())
	# Write this to postgres at this point so that we store the individual distances by year
	write_to_db(distance_df,'distances_over_time')
	
	euclidean_means = distance_df.groupBy(distance_df.locale,distance_df.comp_locale).agg(avg('distance'))
	print('Euclidean means')
	print(euclidean_means.show())
	return euclidean_means
	

# Reduces set of year values to the ones that are the same between the two locations, and returns correlation between them
def compare_years(row):
	ref_dictionary = dict(zip(row[4], row[3]))
	comp_dictionary = dict(zip(row[7], row[6]))
	if(set(row[4]) != set(row[7])):
		ref_keys = set(ref_dictionary.keys())
		comp_keys = set(comp_dictionary.keys())
		for item in ref_dictionary.keys():
  			if not comp_dictionary.has_key(item):
  				del ref_dictionary[item]
  		for item in comp_dictionary.keys():
  			if not ref_dictionary.has_key(item):
  				del comp_dictionary[item]
	correlation = float('nan')
	if len(ref_dictionary) > 0 and len(comp_dictionary) > 0:
		correlation = float(np.corrcoef(ref_dictionary.values(), comp_dictionary.values())[0][1])
	return row[2], row[5], row[0], correlation


# Returns the average correlations over tme between two locations, takes in filtered dataframe of original data
def get_correlations(new_df):
	# Groups each row by location and dimension, and gets vector of year values for each (loc, dimension) 
	correlation_rows = new_df.select('locale','interval','dimension_id','zscore','locale_class').groupBy(new_df.locale,
		new_df.dimension_id,new_df.locale_class).agg(func.collect_list('zscore'),func.collect_list('interval')).withColumnRenamed("collect_list(zscore)", "ref_zscores").withColumnRenamed("collect_list(interval)", "ref_intervals")
	print(correlation_rows.show())
	joined_df = correlation_rows.join(correlation_rows.select('dimension_id','locale_class',
		col('locale').alias('comp_locale'),
		col('ref_zscores').alias('comp_zscores'),
		col('ref_intervals').alias('comp_intervals')), ['dimension_id', 'locale_class'])
	print('In correlations')		
	print(joined_df.show())
	schema = StructType([
		StructField("locale", StringType(), False),
		StructField("comp_locale", StringType(), False),
		StructField("dimension_id", StringType(), False),
		StructField("correlation", FloatType(), False)
		])

	correlations = joined_df.rdd.map(compare_years)
	print(correlations.take(10))
		
	correlation_df = global_settings.sqlContext.createDataFrame(correlations, schema)
	print('Correlations:')
	print(correlation_df.show())
	print(correlation_df.rdd.getNumPartitions())
	correlation_df = correlation_df.repartition('locale')
	corr_means = correlation_df.groupBy(correlation_df.locale,correlation_df.comp_locale).agg(avg(correlation_df.correlation))
	corr_means = corr_means.select('locale','comp_locale','avg(correlation)', ((corr_means['avg(correlation)']*-1/2) + 1 ).alias("adjusted_correlation"))
	print('Correlation means:')
	print(corr_means.show())
	return corr_means


# Get the zscores, distance, correlation and weighted distance between all locations	
def compare_locations(df, locale_class):
	# Remove the values we don't care about	
	filtered_df = df.select('locale','interval','dimension_id','value','locale_class')
	# new_df = new_df.where(col('dimension_labels').isin('Total') == False)
	filtered_df = filtered_df.where(col('locale_class').like('US') == False)
	#new_df = new_df.where(col('locale_class').like('US:ST:PL') == False)
	print(filtered_df.show())
	filtered_df = get_zscore_comparison(filtered_df)
	print(filtered_df.show())
	# Takes in default information with zscores, and returns the average distances over time between locations
	means = get_euclidean_distances(filtered_df)
	# Takes in the default data with zscores added, and returns the average correlations over dimensions between locations
	corrs = get_correlations(filtered_df)
	print(corrs.show())
	# Join our final correlations and distances, and multiply together
	final_joined_locs = means.join(corrs, ['locale','comp_locale'], 'inner')
	final_joined_locs = final_joined_locs.select('locale','comp_locale',
		'avg(distance)', 'avg(correlation)',
		(final_joined_locs['avg(correlation)']* final_joined_locs['avg(distance)'] ).alias("weighted_distance"))
	print(final_joined_locs.show())
	return means

	
	# Next steps:
		# make sure that the distance is doing the same checking as correlations (checkbox) 
		# Figure out if you can make the correlation calculation faster????
		# Make sure that the file can be saved to postgres!!!
		# Add multiplication between distance and correlation (checkbox)
		# Add reading of gzip files and full path to S3 data...

	



config = configparser.ConfigParser()
config.read(sys.argv[1])
connect_to_s3()

