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
	

# returns the mean and stddev of each dimension
def get_dim_stats(df):
	group_stats = df.describe(['value']).collect()
	for row in group_stats:
		if row.summary == 'mean':
			mean = float(row.value)
		if row.summary == 'stddev':
			if row.value == 'nan':
				stddev = 0
			else:
				stddev = float(row.value)
	return mean,stddev

# computes the zscore and euclidean distance for each 
def get_zscore_comparison(df):
	df.describe(['value']).show()
	dims  = df.select('dimension_labels').distinct().collect()

	# Filters table to dimension we want to get mean and average for
	# doing this way for now..
	for dim in dims:
		dimension_df = df.where(col("dimension_labels").isin(dim.dimension_labels))
		print('Dimension DF:')
		dimension_df.show()
		# figure out how to handle if stddev cannot be computed later, right now we probably won't have any actual instances of this
		if dimension_df.count() > 1:	
			mean,stddev = get_dim_stats(dimension_df)
			# takes in dataframe limited to single dimension, returns key of location, value of zscore
			# reduces by zscore to produce vectors like so [(US:CA -> (zscore, zscore, zscore)
			zscores = dimension_df.rdd.map(lambda row: (row[2],(float(row[5]) - mean) / stddev)).groupByKey().map(lambda x: (x[0], list(x[1])))
			# makes an n*n comparison matrix for every vector in our set 
			location_zscores = zscores.cartesian(zscores)
			print(zscores.take(5))
			print(location_zscores).take(5)
		
			# returns the cartesian grouping of every location, zscore vector
			# [(US:CA -> (zscore, zscore, zscore), US:WA (zscore, zscore, zscore)), (), ()....]
			# row[0][0], row[1][0] are our location keys
			# row[0][1], row[1][1] is the zscore vector
			# We have a list of tuples containing all vectors to compare:
				# for every tuple return a tuple of the location comparison as the key
				# and the euclidean distance between the two location values as the value
			euclid_dist = location_zscores.map(lambda row: ( (row[0][0], row[1][0] ), spatial.distance.euclidean (list(row[0][1]), list(row[1][1])) if len(list(row[0][1])) == len(list(row[1][1])) else False ))
			# get correlations between the two vectors
			# returns the 
			correlations = location_zscores.map(lambda row: ( (row[0][0], row[1][0] ), spatial.distance.cosine (list(row[0][1]), list(row[1][1])) if len(list(row[0][1])) == len(list(row[1][1])) else False ))

			print(euclid_dist.take(10))
			print(correlations.take(10))




	# now for each value in each dimension, we'll calculate the zscore given the dimension stddev and mean 
	
	#print(dimension_df.show())
	
	# size: 201428
	# size 33871
	# row[2] is location, row[5] is the value, calculates zscore for each value and returns loc, zscore 
	print('double iterate over all vectors')
	
	#final_rdd = global_settings.sc.union([zscores,zscores])
	# Here's where we reach a bottleneck: what's the best way to compare n*n rows??
	
	print('flatmap Result:  ')


	return euclid_dist



def euclidean(ref,val):
	print(ref)
	if len(list(ref)) != len(list(val)):
		return False
	else:
		return spatial.distance.euclidean(list(ref),list(val))


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
	


	
	return new_df
	
	