# LiveStories data pipeline 

#### Project
Create a data pipeline between LiveStories' S3 tsv source data and a developed data science/analytics infrastructure.  

#### Current problem 
Currently LiveStories data is only available for internal use in tsv format or via API, both of which are not optimal for running data science queries on this ~100GB dataset.  

#### Project outline
During this project, I will:
- transmit the original data set (and reguarly updated data in batch) to Microsoft Azure 
- create a data store that is better suited to run data science queries direclty on the dataset
- Provide a SQL interface (or something similar) for this data store that is easy for a data scientist to query for future metrics. 
- calculate the similarity metrics for all indicators in the original dataset and return all calculated results to the original data store for use by LiveStories' front end API. 
- Automate the process for a newly updated dataset to go through this pipeline and return calculated metrics to the original data source.

Load testing opportunities:
- load test with 10x (100x?) the current dataset, as the company is planning to have data growth

