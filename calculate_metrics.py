from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import boto3
import configparser
import pyspark
import sys
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.column import Column, _to_java_column, _to_seq
import global_settings 
import numpy as np
import pandas as pd
from scipy import spatial
from pyspark.sql.functions import lit
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
	


def get_zscore_comparison(df):
	print('before filtering')
	df.describe(['value']).show()

	#df.select('value').groupBy().mean().show()
	# Get the average values for this dimension
	#average = df.select('value').groupBy().mean().head()['avg(value)']	

	# Filters table to dimension we want to get mean and average for
	print('after filtering')
	#dimension_df = df.where(col("dimension_labels").isin(dim.dimension_labels))
	# now for each value in each dimension, we'll calculate the zscore given the dimension stddev and mean 

	group_stats = df.describe(['value']).collect()
	for row in group_stats:
		if row.summary == 'mean':
			mean = float(row.value)
		if row.summary == 'stddev':
			stddev = float(row.value)
	#print(dimension_df.show())
	
	
	# schema = StructType([StructField("x", IntegerType(), True)])
	# row[2] is location, row[5] is the value, calculates zscore for each value and returns loc, zscore 
	# size: 201428
	# size 33871
	zscores = df.rdd.map(lambda row: (row[2],(float(row[5]) - mean) / stddev)).groupByKey().map(lambda x: (x[0], list(x[1])))
	print('double iterate over all vectors')
	
	#final_rdd = global_settings.sc.union([zscores,zscores])
	# Here's where we reach a bottleneck: what's the best way to compare n*n rows??
	
	print('flatmap Result:  ')

	# returns the cartesian grouping of every location, zscore vector
	# [(US:CA -> (zscore, zscore, zscore), US:WA (zscore, zscore, zscore)), (), ()....]
	location_zscores = zscores.cartesian(zscores)
	print(location_zscores).take(5)
	# row[0][0], row[1][0] are our location keys
	# row[0][1], row[1][1] is the zscore vector
	# if we have a list of tuples:
		# for every tuple return a tuple of the location comparison as the key
		# and the euclidean distance between the two location values as the value
	euclid_dist = location_zscores.map(lambda row: ( (row[0][0], row[1][0] ), spatial.distance.euclidean (list(row[0][1]), list(row[1][1])) if len(list(row[0][1])) == len(list(row[1][1])) else False ))
	print(euclid_dist.take(10))
	return euclid_dist



def euclidean(ref,val):
	print(ref)
	if len(list(ref)) != len(list(val)):
		return False
	else:
		return spatial.distance.euclidean(list(ref),list(val))

def compare_row_to_zscores(row,zscores):
	print('hi')
	print(row.take(10))
	print(zscores.take(10))


def get_zscores(unique_labels,df):
	# for each dimension, get mean and stddev
	dimension_df = df.where(col("dimension_labels").isin(dim.dimension_labels))
	group_stats = dimension_df.describe(['value']).collect()
	for row in group_stats:
		if row.summary == 'mean':
			mean = float(row.value)
		if row.summary == 'stddev':
			stddev = float(row.value)
	# now we have our mean and stddev for each dimension


def compare_locations(df, ref_locale, locale_class, sc,dim_instances = 'all', interval = 'all'):
	new_df = df
	# Drop all rows that have 'Total', as we don't care about comparing them
	new_df = df.where(col('dimension_labels').isin('Total') == False)
	# drop all rows with the locale_class of 'US' because this is the highest level view (there's nothing to compare to)
	new_df = new_df.where(col('locale_class').like('US') == False)
	print('updated dataframe')
	print(new_df.show())


	# reduce to the date range of the reference location (map reduce on location min and max)
	# Get a list of the unique dimensions and intervals
	unique_labels = new_df.select('dimension_labels').distinct()
	unique_times = new_df.select('interval').distinct()
	unique_locations = new_df.select('locale').distinct().show()
	print(unique_locations)
	
	# get date range 
	#min_times = df.rdd.min(lambda (locale,interval): interval).toDF()
	#min_times.show()
	
	print(unique_labels)
	#get_zscores(unique_labels,new_df)
	
	for dim in unique_labels.collect():	
		zscores = get_zscore_comparison(new_df)
		#get_euclidian_dist(zscores,)
	


	#ref = df[location][zscores]
	for loc in unique_locations:

		comparision_vec = df[loc][zscores]
		euc[i] = sp.spatial.distance.euclidean(u,v)

	
	return new_df
	
	