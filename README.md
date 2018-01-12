# LiveStories data pipeline 
Data pipeline between LiveStories' S3 tsv source data and a developed data science/analytics infrastructure.  

Currently LiveStories data is only available for internal use in tsv format or via API, both of which are not optimal for running data science queries on this ~100GB dataset.  

During this project, I will:
- transmit the original data set (and updated data in batch) to Microsoft Azure 
- create a data store that is better suited to run data science queries direclty on the dataset
- calculate the similarity metrics for all indicators in the original dataset and return all calculated results to the original data store 
