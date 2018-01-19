# LiveStories data pipeline 

### Project
Create a data pipeline between LiveStories' S3 tsv source data and a developed data science/analytics infrastructure.  

### Current problem 
Currently LiveStories data is only available for internal use in tsv format or via API, both of which are not optimal for running data science queries on this ~100GB dataset.  

### Project outline
During this project, I will:
- transmit the original data set (and reguarly updated data in batch) to Microsoft Azure 
- create a data store that is better suited to run data science queries direclty on the dataset
- Provide a SQL interface (or something similar) for this data store that is easy for a data scientist to query for future metrics. 
- calculate the similarity metrics for all indicators in the original dataset and return all calculated results to the original data store for use by LiveStories' front end API. 
- Automate the process for a newly updated dataset to go through this pipeline and return calculated metrics to the original data source.

Load testing opportunities:
- load test with 10x (100x?) the current dataset, as the company is planning to have data growth

### Architecture
![alt text](https://github.com/allsway/ls-data-pipeline/blob/master/livestories_pipeline.png)

### Metric results

```
cqlsh:livestories> select  * from adjacency_matrix limit 10;

 indicator_id        | locales  | adj_correlation    | correlation         | raw_distance       | ref_location | weighted_distance
---------------------+----------+--------------------+---------------------+--------------------+--------------+--------------------
 ACS_B08103|US:ST:MN | US:ST:AK | 0.9038867348922401 | 0.19222653021551972 | 2.6326031497226143 |     US:ST:MN |  2.379575065269801
 ACS_B08103|US:ST:MN | US:ST:AL | 0.8558019749932879 |  0.2883960500134241 | 2.0622968620822673 |     US:ST:MN | 1.7649177275924646
 ACS_B08103|US:ST:MN | US:ST:AR | 0.7081450397230449 |  0.5837099205539102 | 1.6194166729331991 |     US:ST:MN | 1.1467818841824415
 ACS_B08103|US:ST:MN | US:ST:AZ | 0.6585897254987574 |  0.6828205490024851 | 2.0594893830896486 |     US:ST:MN | 1.3563585474766169
 ACS_B08103|US:ST:MN | US:ST:CA | 0.7479727164535834 |  0.5040545670928333 |   1.78972228996745 |     US:ST:MN | 1.3386634429244815
 ACS_B08103|US:ST:MN | US:ST:CO | 0.8266527992534867 | 0.34669440149302655 | 2.0256167014101156 |     US:ST:MN |  1.674481716435286

```
