from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
import boto3
import boto
import configparser
import pyspark
import sys
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.column import Column, _to_java_column, _to_seq
import global_settings 
import numpy as np
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
import pyspark.sql.functions as func
from pyspark.sql.types import *
import numpy as np
import pandas as pd
from scipy import spatial
from scipy import stats
import math
import psycopg2
import re

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
	
	
def get_user():
	return config.get('Dataconnector', 'user')

def get_pass():
	return config.get('Dataconnector', 'password')
	
def get_connection():
	return config.get('Dataconnector','data_connection')


# Saves the distances, correlations and weighted correlations to postgres
def write_to_db(df,table):

	print("Writing to db..." + table)
	#print(df.show())
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


# Gets the 
def get_file_contents(file):
	print (file)
	#if file['Key'] is not None:
	#	name = file['Key']
	name = file
	if 'tsv.gz' in name and 'ACS' not in name:
		indicator_id = name.split('.')[0]
		filename = 's3a://' + get_bucket() +  '/' + name.encode('utf-8')
		#filename =  's3a://' +  name
		print(filename)
			
		# read in data without headers from S3
		schema2 = StructType([
			StructField("indicator_id", StringType(), False),
			StructField("locale", StringType(), False),
			StructField("interval", StringType(), False),
			StructField("dimension_id", StringType(), False),
			StructField("value", FloatType(), False)
		])

		dataframe = global_settings.sqlContext.read.format('com.databricks.spark.csv').options( delimiter='\t', 
			schema=schema2).load(filename)
		#print (dataframe.show())	
	
		old_columns = dataframe.schema.names
		print(old_columns)
		new_columns = ["indicator_id", "locale", "interval","dimension_id","value"]
		# Set the column headers for the file
		dataframe = dataframe.select(col("_c0").alias("indicator_id"),
			 col("_c1").alias("locale"),
			 col("_c2").alias("interval"),
			 col("_c3").alias("dimension_id"),
			 col("_c4").alias("value"))
		#print(dataframe.show())
		compare_locations(dataframe,indicator_id)


# Handle files
def get_files(objects):
	#print(objects)
	list = objects['Contents']
	list = ['digested/data/COM.POLICYMAP.US.GOV.US.DOC.CENSUS.FASTFOOD/observations/COM.POLICYMAP.US.GOV.US.DOC.CENSUS.FASTFOOD:REST.tsv.gz',
'digested/data/COM.POLICYMAP.US.GOV.US.DOC.CENSUS.FASTFOOD/observations/COM.POLICYMAP.US.GOV.US.DOC.CENSUS.FASTFOOD:RESTPEOPLERATE.tsv.gz',
'digested/data/COM.POLICYMAP.US.GOV.US.DOC.CENSUS.FASTFOOD/observations/COM.POLICYMAP.US.GOV.US.DOC.CENSUS.FASTFOOD:RESTRATE.tsv.gz'
]
	distance_matrix = map(get_file_contents, list)


# Connect to our S3 storage 
def connect_to_s3():
	conf = SparkConf().setAppName('text')
	global_settings.sc = pyspark.SparkContext()
	global_settings.sqlContext = SQLContext(global_settings.sc)
	calling_format = boto.s3.connection.OrdinaryCallingFormat
	connection = boto.s3.connect_to_region('us-west-2',
       aws_access_key_id=get_key(),
       aws_secret_access_key=get_secret(),
       is_secure=True,               # uncomment if you are not using ssl
       calling_format = boto.s3.connection.OrdinaryCallingFormat()
    )
	bucket = connection.get_bucket(get_bucket())
	client = boto3.client('s3')
	bucket = get_bucket()
	print(bucket)
	objects = client.list_objects(Bucket = bucket)
	get_files(objects)


# Returns a Dataframe with the zscore calculated based on each dimension 
def get_zscore_comparison(df):
	# Get the mean and stddev for each dimension
	mean_df = df.groupBy(df.dimension_id,df.interval).agg(avg('value'),stddev('value'))
	
	#sdev = df.groupBy(df.dimension_id,df.interval).agg(stddev('value'))
	# broadcast join on catalyst join flag 
	df = df.join(broadcast(mean_df), ['interval', 'dimension_id'], 'inner')
	#df = df.join(sdev, ['interval', 'dimension_id'], 'inner')
	df = df.withColumn('stddev', df["stddev_samp(value)"].cast(FloatType()))
	df = df.withColumn('avg', df["avg(value)"].cast(FloatType()))
	df = df.withColumn('value2', df["value"].cast(FloatType()))

	#print(df.show())
	df = df.rdd.map(lambda row: ( row.interval , row.dimension_id, row.indicator_id,
		row.locale, row.value, row.locale_class,  
		float(row.value) if (math.isnan(float(row.stddev)) or float(row.stddev) == 0) else (float(row.value2) - float(row.avg)) / float(row.stddev) ))

	schema = StructType([
		StructField("interval", StringType(), False),
		StructField("dimension_id", StringType(), False),
		StructField("indicator_id", StringType(), False),
		StructField("locale", StringType(), False),
		StructField("value", StringType(), False),
		StructField("locale_class", StringType(), False),
		StructField("zscore", FloatType(), False)
		])

	returned_df = global_settings.sqlContext.createDataFrame(df, schema)
	#print(returned_df.show())
	return returned_df


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
	return row[2], row[5], distance, row[0], row[1]

# Calculate the euclidean distance between all locations in the file
def get_euclidean_distances(new_df):
	# Groups each row by location and interval, and gets n-dimensional vector of values for each (loc, interval) 
	distance_rows = new_df.select('locale','interval','dimension_id','zscore','indicator_id',
		'locale_class').groupBy(new_df.locale,
		new_df.interval,new_df.indicator_id,new_df.locale_class).agg(func.collect_list('zscore'),
		func.collect_list('dimension_id')).withColumnRenamed("collect_list(zscore)", "ref_zscores").withColumnRenamed("collect_list(dimension_id)", "ref_dimensions")
	print('Chceking distances...')
	# Join the dataframe to itself on interval, resulting in a location x location table
	distance_rows = distance_rows.join(distance_rows.select('interval', 'indicator_id','locale_class',
		col('locale').alias('comp_locale'),
		col('ref_zscores').alias('comp_zscores'),
		col('ref_dimensions').alias('comp_dimensions')), ['interval','indicator_id','locale_class']) #.sort(col('locale').desc())
	distance_rows = distance_rows.drop('locale_class')
	print(distance_rows.rdd.getNumPartitions())
	checked_distances = distance_rows.rdd.map(compare_dimensions)
	print('Distances after verification')
		
	schema = StructType([
		StructField("locale", StringType(), False),
		StructField("comp_locale", StringType(), False),
		StructField("distance", FloatType(), False),
		StructField("interval", StringType(), False),
		StructField("indicator_id", StringType(), False),
		])

	distance_df = global_settings.sqlContext.createDataFrame(checked_distances, schema)
	#print(distance_df.show())
	# Write this to postgres at this point so that we store the individual distances by year
	#distance_df.rdd.persist()
	#distance_df.repartition('locale') # this might be redundant
	distance_df.rdd.persist()
	write_to_db(distance_df,'distances_over_time')
	
	euclidean_means = distance_df.groupBy(distance_df.locale,
		distance_df.indicator_id,distance_df.comp_locale).agg(avg('distance'), func.collect_list('interval')) 
	return euclidean_means	

# Reduces set of year values to the ones that are the same between the two locations, and returns correlation between them
def compare_years(row):
	ref_dictionary = dict(zip(row[4], row[3]))
	comp_dictionary = dict(zip(row[7], row[6]))
	if len(ref_dictionary) < 5 and len(comp_dictionary) < 5:
		return row[2], row[5], row[0], float(0), row[1]
	if(set(row[4]) != set(row[7])):
		ref_keys = set(ref_dictionary.keys())
		comp_keys = set(comp_dictionary.keys())
		for item in ref_dictionary.keys():
  			if not comp_dictionary.has_key(item):
  				del ref_dictionary[item]
  		for item in comp_dictionary.keys():
  			if not ref_dictionary.has_key(item):
  				del comp_dictionary[item]
	correlation = float(0)
	if len(ref_dictionary) > 4 and len(comp_dictionary) > 4:
		correlation = float(np.corrcoef(ref_dictionary.values(), comp_dictionary.values())[0][1])
	return row[2], row[5], row[0], correlation, row[1]

# Returns the average correlations over tme between two locations, takes in filtered dataframe of original data
def get_correlations(new_df):
	# Let's trim our dimension column, which currently includes the study ID 
	new_df = new_df.withColumn('dimension_id',func.split(new_df.dimension_id, '>')[1])
	print('After split:')
	#print(new_df.show())
	# Groups each row by location and dimension, and gets vector of year values for each (loc, dimension) 
	correlation_rows = new_df.select('locale','interval','dimension_id',
		'zscore','indicator_id','locale_class').groupBy(new_df.locale,
		new_df.dimension_id,new_df.indicator_id, 
		new_df.locale_class).agg(func.collect_list('zscore'),
		func.collect_list('interval')).withColumnRenamed("collect_list(zscore)", 'ref_zscores').withColumnRenamed("collect_list(interval)", "ref_intervals")
	
	# Creates a joined set of location to every other location pairs on 
	joined_df = correlation_rows.join(correlation_rows.select('dimension_id','indicator_id', 'locale_class',
		col('locale').alias('comp_locale'),
		col('ref_zscores').alias('comp_zscores'),
		col('ref_intervals').alias('comp_intervals')), ['dimension_id', 'indicator_id','locale_class'])
	joined_df = joined_df.drop('locale_class')
	#print(joined_df.show())
	
	# Compares the the values over a set of years, and reduces each vector to the intersecting set of years
	# Returns the correlations for each location/location pair 
	correlations = joined_df.rdd.map(compare_years)
	#print(correlations.take(10))
		
	schema = StructType([
		StructField("locale", StringType(), False),
		StructField("comp_locale", StringType(), False),
		StructField("dimension_id", StringType(), False),
		StructField("correlation", FloatType(), False),
		StructField("indicator_id", StringType(), False)
		])
	
	correlation_df = global_settings.sqlContext.createDataFrame(correlations, schema)
	correlation_df.rdd.persist()
	#print(correlation_df.show())
	# might want to persist here
	write_to_db(correlation_df,'correlations_by_dimension')

	print('Correlations:')
	print(correlation_df.rdd.getNumPartitions())
	correlation_df = correlation_df.repartition('locale')
	corr_means = correlation_df.groupBy(correlation_df.locale,
		correlation_df.comp_locale,correlation_df.indicator_id).agg(avg(correlation_df.correlation), 
		func.collect_list('dimension_id'))
	corr_means = corr_means.select('locale','comp_locale','indicator_id','collect_list(dimension_id)', 
		((corr_means['avg(correlation)']*-1/2) + 1 ).alias("adjusted_correlation"))
	print('Correlation means:')
	#print(corr_means.show())
	return corr_means

# Get the locale class from the locale field
def python_regex(row):
  	p = re.compile('(:[^:]+):[^:]+')
  	return p.sub(r'\1', row[1]), row[1]

# Add the locale class column to our dataframe 
def add_in_locale_class(filtered_df):
	locale_class_col = filtered_df.rdd.map(python_regex)
	schema = StructType([
		StructField("locale_class", StringType(), False),
		StructField("locale", StringType(), False)
	])
	locale_class_df = global_settings.sqlContext.createDataFrame(locale_class_col, schema)
	filtered_df = filtered_df.join(locale_class_df,'locale','inner')
	return filtered_df

# Get the zscores, distance, correlation and weighted distance between all locations	
def compare_locations(df, locale_class):
	filtered_df = df.where(col('locale').like('US') == False) # Remove values that only correspond to US (nothing to compare to)
	filtered_df = add_in_locale_class(filtered_df)
	filtered_df = get_zscore_comparison(filtered_df)
	#filtered_df.persist()
	# Takes in the 5 column data file with the addition of zscores, and returns the average distances over time between locations
	means = get_euclidean_distances(filtered_df)
	# Takes in the default data with zscores added, and returns the average correlations over dimensions between locations
	corrs = get_correlations(filtered_df)
	#print(corrs.show())
	# Join our final correlations and distances, and multiply together
	final_joined_locs = means.join(corrs, ['locale','comp_locale','indicator_id'], 'inner')
	#print(final_joined_locs.show())
	final_joined_locs = final_joined_locs.select('locale','comp_locale',
		'adjusted_correlation', 
		col('collect_list(interval)').alias('compared_intervals'), 
		 col('collect_list(dimension_id)').alias('compared_dimensions'),
		(final_joined_locs['adjusted_correlation']*final_joined_locs['avg(distance)'] ).alias("weighted_distance"),
		'indicator_id')

	#print(final_joined_locs.show())
	write_to_db(final_joined_locs,'weighted_distances')

	
	# Next steps:
		# Figure out if you can make the correlation calculation faster????
		# Add reading of gzip files and full path to S3 data...

	


config = configparser.ConfigParser()
config.read(sys.argv[1])
connect_to_s3()

