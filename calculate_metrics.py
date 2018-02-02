from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import boto3
import configparser
import pyspark
import sys
import math
from pyspark.sql import Row, SQLContext, SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, stddev, avg, udf
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
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.mllib.stat import Statistics
	

# Returns a Dataframe with the zscore calculated based on each dimension 
def get_zscore_comparison(df):
	# Get the mean and stddev for each dimension
	mean_df = df.groupBy(df.dimension_id,df.interval).agg(avg('value'))
	sdev = df.groupBy(df.dimension_id,df.interval).agg(stddev('value'))
	
	df = df.join(mean_df, ['interval', 'dimension_id'], 'inner')
	df = df.join(sdev, ['interval', 'dimension_id'], 'inner')
	df = df.withColumn('stddev', df["stddev_samp(value)"].cast(FloatType()))
	df = df.withColumn('avg', df["avg(value)"].cast(FloatType()))

	schema = StructType([
		StructField("interval", StringType(), False),
		StructField("dimension_id", StringType(), False),
		StructField("locale", StringType(), False),
		StructField("value", FloatType(), False),
		StructField("locale_class", StringType(), False),
		StructField("zscore", FloatType(), False)])

	df = df.rdd.map(lambda row: ( row.interval , row.dimension_id, 
		row.locale, row.value, row.locale_class, 
		row.value if (math.isnan(float(row.stddev)) or float(row.stddev) == 0) else (row.value - row.avg) / row.stddev ))
	print(df.take(10))

	returned_df = global_settings.sqlContext.createDataFrame(df, schema)

	print(returned_df.show())
	return returned_df


def write_to_db(df):
	print('his')


def compare_dimensions(row):
	print('Hi')
	if set(row.ref_dimensions) == set(row.comp_dimensions):
		return row.ref_zscores,row.comp_zscores
	for i,dim in row.ref_dimensions:
		if dim not in row.comp_dimensions:
			del(row.ref_zscore[i])
	for i,dim in row.comp_dimensions:
		if dim not in row.ref_dimensions:
			del(row.comp_zscores[i])
	print(row.ref_zscores, row.comp_zscores)
	distance = spatial.distance.euclidean(row.ref_zscores, row.comp_zscores)
	return row.locale, row.comp_locale, row.ref_dimensions, row.comp_dimensions, row.ref_zscores, row.comp_zscores, distance
	



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
	
	
	schema = StructType([
		StructField("locale", StringType(), False),
		StructField("locale2", StringType(), False),
		StructField("interval", StringType(), False),
		StructField("distance", FloatType(), False)])
		
	#distance_test = distance_rows.rdd.map(lambda row: (compare_dimensions(row)))
	#test_df = distance_rows.rdd.map(lambda row: compare_dimensions(row))
	print('distance test:\n')
	# rows = empty rdd
	
	print(distance_rows.rdd.getNumPartitions())

	not_sure = distance_rows.rdd.map(compare_dimensions)
	print(not_sure.take(5))
	schema2 = StructType([
		StructField("locale", StringType(), False),
		StructField("locale2", StringType(), False),
		StructField("interval", StringType(), False),
		StructField("distance", FloatType(), False)])

	distances = distance_rows.rdd.map(lambda row: (row.locale, 
		row.comp_locale, row.interval, 
		spatial.distance.euclidean(row.ref_zscores, row.comp_zscores,
		) if set(row.ref_dimensions) == set(row.comp_dimensions) else float('nan') ))

	print(distances.take(5))
	distance_df = global_settings.sqlContext.createDataFrame(distances, schema)
	write_to_db(distance_df)
	
	print('Distance dataframe:')
	print(distance_df.show())
	# Write this to S3. 
	
	
	euclidean_means = distance_df.groupBy(distance_df.locale,distance_df.locale2).agg(avg('distance'))
	# Write the distance and year to database?? 
	# ideal world, would want the distances for every time, and then the distances averaged
	print('Euclidean means')
	print(euclidean_means.show())
	return euclidean_means
#	new_df.withColumn(distance_df)
	

# Returns the correlations between two locations
def get_correlations(new_df):
	print('Hi')
	# Groups each row by location and dimension, and gets vector of year values for each (loc, dimension) 
	correlation_rows = new_df.select('locale','interval','dimension_id','zscore','locale_class').groupBy(new_df.locale,
		new_df.dimension_id,new_df.locale_class).agg(func.collect_list('zscore'),func.collect_list('interval')).withColumnRenamed("collect_list(zscore)", "ref_zscores").withColumnRenamed("collect_list(interval)", "ref_intervals")
#	correlation_rows = new_df.select('locale','dimension_id','zscore','interval').groupBy(new_df.locale,new_df.dimension_id)
	print(correlation_rows.show())

	# makes cartesian dataframe joined on dimension
	correlation_rows = correlation_rows.join(correlation_rows.select('dimension_id',
		col('locale').alias('comp_locale'),
		col('ref_zscores').alias('comp_zscores'),
		col('ref_intervals').alias('comp_intervals')), 'dimension_id')
	print('In correlations')
	
	# explode list, turn into vector? 
	#correlation_rows.rdd.map()
	
	print(correlation_rows.show())
	schema = StructType([
		StructField("locale", StringType(), False),
		StructField("locale2", StringType(), False),
		StructField("dimension", StringType(), False),
		StructField("correlation", FloatType(), False)])


	test_schema = StructType([
		StructField("locale", StringType(), False),
		StructField("locale2", StringType(), False),
		StructField("dimension", StringType(), False),
		StructField("correlation", DoubleType(), False)])
		

	#correlations = correlation_rows.rdd.map(lambda row: (row.ref_zscores),func.explode(row.comp_zscores)))

	correlations = correlation_rows.rdd.map(lambda row: (row.locale, 
		row.comp_locale, row.dimension_id,
		np.corrcoef(row[3][0] ,row[6][0])[0][1] ))
		
	#correlations = correlation_rows.rdd.map(lambda row: (row.locale, 
	#	row.comp_locale, row.dimension_id,
	#	spatial.distance.correlation(row.ref_zscores ,row.comp_zscores)) )
	print(correlations.take(5))

	print_test = correlation_rows.rdd.map(lambda row: (row.locale, type(row.ref_zscores), row.comp_zscores))
	#print(print_test.take(5))
	# spell it out 
		
	print(correlations.take(5))


	correlation_df = global_settings.sqlContext.createDataFrame(correlations, schema)
	print('Correlations:')
	#print(correlation_df.show())
	#corr_means = correlation_df.groupBy(correlation_df.locale,correlation_df.locale2).agg(avg('correlation'))
	#print(corr_means.show())
	#return corr_means




# Get the zscores, distance, correlation and weighted distance between all locations	
def compare_locations(df, ref_locale, locale_class):
	# Remove the values we don't care about	
	new_df = df.select('locale','interval','dimension_id','value','locale_class')
	# new_df = new_df.where(col('dimension_labels').isin('Total') == False)
	new_df = new_df.where(col('locale_class').like('US') == False)
	#new_df = new_df.where(col('locale_class').like('US:ST:PL') == False)

	print(new_df.show())
	new_df = get_zscore_comparison(new_df)
	print(new_df.show())
	
	means = get_euclidean_distances(new_df)
	#corrs = get_correlations(new_df)

	# Join our final correlations and distances, and multiply together
	#joined_locations = means.join(corrs, ['locale','locale2'],'inner')
	#print(joined_locations.show())
	return means

	
	
	
	#print('Num partititons')
	#print(new_df.rdd.getNumPartitions())

	#new_df = new_df.repartition('dimension_id')
	#print('After repartitioning')
	#print(new_df.rdd.getNumPartitions())
		# Testing partitioning
	#print(new_df.rdd.getNumPartitions())
	
	
	## For each partition: 
		## For each subset of the file 

	