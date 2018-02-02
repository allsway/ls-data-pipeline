from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import boto3
import configparser
import pyspark
import sys
from pyspark.sql import Row, SQLContext, SparkSession
from pyspark import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import col
import pyspark.sql.functions as func
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
	
		
# Returns the correlations for a 
#def get_correlations(ref,zscores,location_dimensions,location_dates):



# (loc, dimension) =>[y1 value, y2 value, y3 value] for CORRELATIONS
# (loc, y) => [dim value, dim2 value, dim3 value, dim4 value] for DISTANCES 

#def get_euc_distances(ref,zscores,location_dimensions,location_dates):

# computes the zscore and euclidean distance for each 
def get_zscore_comparison(df):
	dims  = df.select('dimension_id').distinct().collect()
	times = df.select('interval').distinct().collect()
	
	# Filters table to dimension we want to get mean and average for
	# doing this way for now..
	dist_zscores, corr_zscores = global_settings.sc.emptyRDD(), global_settings.sc.emptyRDD()
	window_spec = Window.partitionBy(df['dimension_id','value'])
	print(window_spec)
	
	test_df = func.max(df['value']).over(window_spec)
	print(test_df)
	
	for dim in dims:
		dimension_df = df.where(col("dimension_id").isin(dim.dimension_id))
		for time in times: 
			time_dim_df = dimension_df.where(col("interval").isin(time.interval))
			if time_dim_df.count() > 1:	
				mean,stddev = get_dim_stats(time_dim_df)

	return dist_zscores,corr_zscores




# MUST CALCULATE MEAN AND STDDEV within each dimension
# 
# (loc1, loc2) loc1 - 2010, 2011, 2015, loc2 - 2010, 2015
# 
# 

	
def compare_locations(df, ref_locale, locale_class, sc,dim_instances = 'all', interval = 'all'):
	# map by each locale class 
	
	new_df = df
	# Drop all rows that have 'Total', as we don't care about comparing them
	new_df = df.where(col('dimension_labels').isin('Total') == False)
	# drop all rows with the locale_class of 'US' because this is the highest level view (there's nothing to compare to)
	new_df = new_df.where(col('locale_class').like('US') == False)
	print('updated dataframe')
	print(new_df.show())
	location_dimensions = new_df.rdd.map(lambda row: ((row[2]), row[4] )).groupByKey().map(lambda x: (x[0], list(x[1])))
	location_dates = new_df.rdd.map(lambda row: ((row[2]), row[3] )).groupByKey().map(lambda x: (x[0], list(x[1])))
	dimensions = global_settings.sc.broadcast(location_dimensions.collectAsMap())
	dates = global_settings.sc.broadcast(location_dates.collectAsMap())

	# for each location, get interval and dimensions, and compare to all other locations
	# dimension_df.rdd.map(lambda row: ((row[2] ),(row[3],dim.dimension_labels,(row[5]) if stddev == 0 else (float(row[5]) - mean) / stddev) )).groupByKey().map(lambda x: (x[0], list(x[1]))) 
	dist_zscores,corr_zscores = get_zscore_comparison(new_df)
	#location_distances = get_euc_distances(zscores,location_dimensions,location_dates)
#	euc_averages, corr_averages = global_settings.sc.emptyRDD(), global_settings.sc.emptyRDD()




	# get (Location key values)

#	for ref in df.select('locale').distinct().collect():
#		dims = dimensions.value[ref.locale]
#		date_range = dates.value[ref.locale]
#		euc_averages = euc_averages.union(get_euc_distances(ref,dist_zscores,dims,date_range))
#		corr_averages = corr_averages.union(get_correlations(ref,corr_zscores,dims,date_range))
	
#	return euc_averages, ref_locale
	
	