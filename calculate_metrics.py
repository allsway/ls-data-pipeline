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
from scipy import stats
from scipy.stats.stats import pearsonr   
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
			stddev = float(row.value)
	#if stddev == 0:
	#	stddev =  0.0000000000000001
	return mean,stddev



# returning overlap as the final 


# computes the zscore and euclidean distance for each 
def get_zscore_comparison(df):
	df.describe(['value']).show()
	dims  = df.select('dimension_labels').distinct().collect()

	# Filters table to dimension we want to get mean and average for
	# doing this way for now..
	# replace the values in the rdd with zscore values 
	zscores = global_settings.sc.emptyRDD()
	for dim in dims:
		dimension_df = df.where(col("dimension_labels").isin(dim.dimension_labels))
		if dimension_df.count() > 1:	
			mean,stddev = get_dim_stats(dimension_df)
			# takes in dataframe limited to single dimension, returns key of location, year, value of zscore
			# reduces by zscore to produce vectors like so [(US:CA, y1 -> (zscore, zscore, zscore)
			print(mean, stddev)
			if stddev != 0:	
				dim_zscores = dimension_df.rdd.map(lambda row: ((row[2], row[3]),(row[5] if stddev == 0 else (float(row[5]) - mean) / stddev) )).groupByKey().map(lambda x: (x[0], list(x[1]))) 
			zscores = zscores.union(dim_zscores)
	print('All zscores returned')
	print(zscores.take(20))
	# returns an n*n comparison matrix for every (loc, loc, y1, y2) vector in our set 
	location_zscores = zscores.cartesian(zscores)
	print('zscores by location and date')
	print(location_zscores).take(5)

	# returns the cartesian grouping of every location, zscore vector
	# [(US:CA -> (zscore, zscore, zscore), US:WA (zscore, zscore, zscore)), (), ()....]
	# row[0][0], row[1][0] are our location keys
	# row[0][1], row[1][1] is the zscore vector
	# We have a list of tuples containing all vectors to compare:
		# for every tuple return a tuple of the location comparison as the key
		# and the euclidean distance between the two location values as the value
	#euclid_dist = location_zscores.map(lambda row: ( (row[0][0], row[1][0] ), spatial.distance.euclidean (list(row[0][1]), list(row[1][1])) if len(list(row[0][1])) == len(list(row[1][1])) else False ))
	euclid_dist = location_zscores.map(lambda row: ( (row[0][0][0], row[1][0][0], row[0][0][1], row[1][0][1] ), spatial.distance.euclidean (list(row[0][1]), list(row[1][1])) if len(list(row[0][1])) == len(list(row[1][1])) else False ))
	print('Euclid dists: ')
	print(euclid_dist.take(20))
	# get the average for the (location/time) => distance values, returning location => distance
	
	# When you return the mean of the distances, also reduce the time key to the date range 
	print('Euclid averages')
	#euclid_avgs = euclid_dist.reduceByKey(lambda (row[0][0], row[1][0]), val: sum(val) / len(val))		
	#print(euclid_avgs.take(5))

	#averageByKey = euclid_dist.map(lambda row: ((row[0][0], row[1][0] ): (label, value_sum / count))
	# get the 
	
	
	# get correlations between the two vectors
	correlations = location_zscores.map(lambda row: ( (row[0][0], row[1][0] ), np.corrcoef (list(row[0][1]), list(row[1][1]))[1,0] if len(list(row[0][1])) == len(list(row[1][1])) else False ))
	#correlations.map(mean()*-1 / 2) + 1.0 )
	#print('Correlations')
	#print(correlations.take(100))
	#print('Correlations count')
	return euclid_dist




def euclidean(ref,val):
	print(ref)
	if len(list(ref)) != len(list(val)):
		return False
	else:
		return spatial.distance.euclidean(list(ref),list(val))


# MUST CALCULATE MEAN AND STDDEV within each dimension


def compare_locations(df, ref_locale, locale_class, sc,dim_instances = 'all', interval = 'all'):
	# map by each locale class 
	
	new_df = df
	# Drop all rows that have 'Total', as we don't care about comparing them
	new_df = df.where(col('dimension_labels').isin('Total') == False)
	# drop all rows with the locale_class of 'US' because this is the highest level view (there's nothing to compare to)
	new_df = new_df.where(col('locale_class').like('US') == False)
	print('updated dataframe')
	print(new_df.show())

	#print(unique_locations)
	# map each interval, get min and max -> reduce by location so that we have the min and max interval for each location

	# map each locale_class and perform all operations on locale class groupings
	
	 
	
	
	# get date range 
	#min_times = df.rdd.min(lambda (locale,interval): interval).toDF()
	#min_times.show()
	
	#print(unique_labels)
	#get_zscores(unique_labels,new_df)
	
	#for dim in unique_labels.collect():	
	zscores = get_zscore_comparison(new_df)
		#get_euclidian_dist(zscores,)
	


	
	return zscores, ref_locale
	
	