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
	
		
# Returns the correlations for a 
def get_correlations(ref,zscores,location_dimensions,location_dates):
	ref_locale = zscores.filter(lambda x: (x[0][0] == ref.locale))
	ref_locale = ref_locale.map(lambda x: ((x[0][0], x[0][1]),x[1][1] )).groupByKey().map(lambda x: (x[0], list(x[1])))
	print('Reference location vector:')
	print(ref_locale.take(20))
	# filter out dates and dimensions that don't match our reference location
	total_comparisons = zscores.filter(lambda x: (x[0][1] in location_dates))
	total_comparisons = total_comparisons.filter(lambda x: (x[1][0] in location_dimensions))
	total_comparisons = total_comparisons.filter(lambda x: (x[0][0] != ref.locale))
	total_comparisons = total_comparisons.map(lambda x: ((x[0][0], x[0][1]), x[1][1])).groupByKey().map(lambda x: (x[0], list(x[1])))
	reverse_loc = ref_locale.map(lambda x: ((x[0][1]), (x[0][0], x[1] )))
	reverse_comps = total_comparisons.map(lambda x: ((x[0][1]), (x[0][0], x[1] )))
	print(reverse_loc.take(20))
	joined = reverse_loc.join(reverse_comps)


# (loc, dimension) =>[y1 value, y2 value, y3 value] for CORRELATIONS
# (loc, y) => [dim value, dim2 value, dim3 value, dim4 value] for DISTANCES 

def get_euc_distances(ref,zscores,location_dimensions,location_dates):

	#dimensions = global_settings.sc.broadcast(location_dimensions.collectAsMap())
	#dates = global_settings.sc.broadcast(location_dates.collectAsMap())

	# just this reference location vectors	
	#ref_locale = zscores.map(lambda x: (x[1][0][0], x[1][0][0]))
	# returns an n*n comparison matrix for every (loc, loc, y1, y2) vector in our set 
	# returns the cartesian grouping of every location, zscore vector
	print('Returned zscores:')
	print(zscores.take(10))
	print(zscores.count())
	print(zscores.map(lambda x: ((x[0][0]), x[0] )))
	ref_locale = zscores.filter(lambda x: (x[0][0] == ref.locale))
	ref_locale = ref_locale.map(lambda x: ((x[0][0], x[0][1]),x[1][1] )).groupByKey().map(lambda x: (x[0], list(x[1])))
	print('Reference location vector:')
	print(ref_locale.take(20))
	# filter out dates and dimensions that don't match our reference location
	total_comparisons = zscores.filter(lambda x: (x[0][1] in location_dates))
	total_comparisons = total_comparisons.filter(lambda x: (x[1][0] in location_dimensions))
	total_comparisons = total_comparisons.filter(lambda x: (x[0][0] != ref.locale))
	total_comparisons = total_comparisons.map(lambda x: ((x[0][0], x[0][1]), x[1][1])).groupByKey().map(lambda x: (x[0], list(x[1])))
	
	print('Final sets to compare:')
	print(ref_locale.take(20))
	print(total_comparisons.take(20))

	reverse_loc = ref_locale.map(lambda x: ((x[0][1]), (x[0][0], x[1] )))
	reverse_comps = total_comparisons.map(lambda x: ((x[0][1]), (x[0][0], x[1] )))
	print('Joined tuples?')
	joined = reverse_loc.join(reverse_comps)
	print(joined.take(10))
		
	#location_zscores = zscores.cartesian(zscores)
	#print('zscores by location and date')
	#print(location_zscores).take(10)

	# [(US:CA -> (zscore, zscore, zscore), US:WA (zscore, zscore, zscore)), (), ()....]
	# row[0][0], row[1][0] are our location keys
	# row[0][1], row[1][1] is the zscore vector
	# We have a list of tuples containing all vectors to compare:
		# for every tuple return a tuple of the location comparison as the key
		# and the euclidean distance between the two location values as the value
	
	#filtered_vectors = location_zscores.filter(lambda row: row[1][0]  in (dates.value)[str(row[1][0])])

	show_map = joined.map(lambda row: ((row[1][0][0], row[1][1][0], row[0]), row[1][0][1]))
		
	euclid_dist = joined.map(lambda row: ( (row[0], row[1][0][0], row[1][1][0] ), 
		spatial.distance.euclidean (row[1][0][1], row[1][1][1]) if len(row[1][0][1]) == len(row[1][1][1]) else False ))
	
	print('Euclid dists: ')
	print(euclid_dist.count())
	print(euclid_dist.take(10))
	
	# When you return the mean of the distances, also reduce the time key to the date range 
	print('Euclid averages')
	reduction = euclid_dist.reduceByKey(lambda row: row[0][0])
	print(reduction.take(5))
	reduction = euclid_dist.map(lambda row: ((row[0][1], row[0][2]), row[1] )).groupByKey().map(lambda x: (x[0], np.mean(list(x[1]))))
	print(reduction.take(4))
#	euclid_avgs = euclid_dist.map(lambda row: ((row[0][0][0], row[0][1][0]), row[1]))	
#	 row[0][0] is first location, row[0][1] is second location
#	euc_sums = euclid_dist.combineByKey(lambda value: (value, 1),
#                             lambda x, value: (x[0] + value, x[1] + 1),
#                             lambda x, y: (x[0] + y[0], x[1] + y[1]))

#	print(euc_sums.take(5))
	



# key = (location, year) => [(dim1, val), (dim2, val), (dim3, val)]
# (location,location pair)
# (year, [(dimension, value)(dimension, value)], year2 (dimension, value), (dimension, value) )

# computes the zscore and euclidean distance for each 
def get_zscore_comparison(df):
	dims  = df.select('dimension_id').distinct().collect()
	times = df.select('interval').distinct().collect()
	# Filters table to dimension we want to get mean and average for
	# doing this way for now..
	dist_zscores, corr_zscores = global_settings.sc.emptyRDD(), global_settings.sc.emptyRDD()
	for dim in dims:
		dimension_df = df.where(col("dimension_id").isin(dim.dimension_id))
		for time in times: 
			time_dim_df = dimension_df.where(col("interval").isin(time.interval))
			if time_dim_df.count() > 1:	
				mean,stddev = get_dim_stats(time_dim_df)
				# For correlations, produces vector like so (US:CA, dim1) -> [zscore, zscore, zscore], (US:CA, dim2) -> [zscore, zscore, zscore]
				# For distances, produces vector like so (US:CA, y1) -> [(zscore, zscore, zscore)], (US:CA, y2) -> [(zscore,zscore,zscore)]
				print(mean, stddev)
				d_zscores = dimension_df.rdd.map(lambda row: (((row[2], row[3]), (row[4], row[5]) if stddev == 0 else (row[4],(float(row[5]) - mean) / stddev) )))
				c_zscores = dimension_df.rdd.map(lambda row: (((row[2], row[4]), (row[3], row[5]) if stddev == 0 else (row[3],(float(row[5]) - mean) / stddev) )))
				#dim_zscores = dimension_df.rdd.map(lambda row: ((row[2], row[3, row[4]] ),((row[5]) if stddev == 0 else (float(row[5]) - mean) / stddev) )).groupByKey().map(lambda x: (x[0], list(x[1]))) 
				print(dim.dimension_id)
				dist_zscores = dist_zscores.union(d_zscores)
				corr_zscores = corr_zscores.union(c_zscores)
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
	
	for ref in df.select('locale').distinct().collect():
		dims = dimensions.value[ref.locale]
		date_range = dates.value[ref.locale]
		euc_averages = get_euc_distances(ref,dist_zscores,dims,date_range)
		
	
	return euc_averages, ref_locale
	
	