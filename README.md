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
![alt text](https://github.com/allsway/ls-data-pipeline/blob/master/imgs/livestories_pipeline.png)

### Metric example

```
cqlsh:livestories> select * from adjacency_matrix  limit 24 ;

 indicator_id        | locales  | adj_correlation    | correlation         | raw_distance        | ref_location | weighted_distance
---------------------+----------+--------------------+---------------------+---------------------+--------------+---------------------
 ACS_B08135|US:ST:MN | US:ST:AK |  0.747969823094121 |  0.5040603538117582 |   2.452667452198033 |     US:ST:MN |  1.8345212403292712
 ACS_B08135|US:ST:MN | US:ST:AL | 0.7655599173998975 | 0.46888016520020503 |  0.8234527469502501 |     US:ST:MN |  0.6304024169379521
 ACS_B08135|US:ST:MN | US:ST:AR | 0.7190412322490914 |  0.5619175355018171 |  1.5436736394759274 |     US:ST:MN |  1.1099649959192106
 ACS_B08135|US:ST:MN | US:ST:AZ | 0.7140336898360651 |  0.5719326203278697 | 0.42350686757471223 |     US:ST:MN | 0.30239817132528557
 ACS_B08135|US:ST:MN | US:ST:CA | 0.5705138520463173 |  0.8589722959073655 |  13.858828781332212 |     US:ST:MN |    7.90665379288821
 ACS_B08135|US:ST:MN | US:ST:CO | 0.6440652054054914 |  0.7118695891890173 | 0.39876753566917894 |     US:ST:MN | 0.25683229476981134
 ACS_B08135|US:ST:MN | US:ST:CT | 0.7384502551880221 |  0.5230994896239559 |  1.0629639978645353 |     US:ST:MN |  0.7849460354787463
 ACS_B08135|US:ST:MN | US:ST:DE | 0.6768520183094948 |  0.6462959633810104 |  2.3798859615056096 |     US:ST:MN |  1.6108306163915045
 ACS_B08135|US:ST:MN | US:ST:FL | 0.5885196074108887 |  0.8229607851782225 |   5.667319273607105 |     US:ST:MN |  3.3353285139754165
 ACS_B08135|US:ST:MN | US:ST:GA | 0.6121709408465811 |  0.7756581183068378 |  1.7692053518272577 |     US:ST:MN |   1.083056104778899
 ACS_B08135|US:ST:MN | US:ST:HI |  0.718363405647438 |  0.5632731887051241 |   2.162518985166048 |     US:ST:MN |  1.5534745029611234
 ACS_B08135|US:ST:MN | US:ST:IA | 0.5843659589335413 |  0.8312680821329174 |  1.2862441729176395 |     US:ST:MN |  0.7516373095296961
 ACS_B08135|US:ST:MN | US:ST:ID | 0.8025548090127115 | 0.39489038197457693 |  2.1065607028631512 |     US:ST:MN |  1.6906304225600195
 ACS_B08135|US:ST:MN | US:ST:IL | 0.7015536848971994 |  0.5968926302056012 |  3.6162944221130218 |     US:ST:MN |  2.5370246775065786
 ACS_B08135|US:ST:MN | US:ST:IN | 0.5578483633625296 |  0.8843032732749407 |  0.2958102414167363 |     US:ST:MN | 0.16501725904020112
 ACS_B08135|US:ST:MN | US:ST:KS | 0.6747891340339119 |  0.6504217319321761 |   1.412060283596647 |     US:ST:MN |  0.9528429359718615
 ACS_B08135|US:ST:MN | US:ST:KY | 0.6683844549178016 |  0.6632310901643967 |  0.9115361589754006 |     US:ST:MN |  0.6092565987546398
 ACS_B08135|US:ST:MN | US:ST:LA | 0.7272459080674935 |  0.5455081838650131 |  0.8913913817475992 |     US:ST:MN |  0.6482607348625705
 ACS_B08135|US:ST:MN | US:ST:MA | 0.6044638184326337 |  0.7910723631347325 |  1.0345868704742731 |     US:ST:MN |  0.6253703302271478
 ACS_B08135|US:ST:MN | US:ST:MD | 0.6031586200554094 |  0.7936827598891811 |   1.394697367654748 |     US:ST:MN |  0.8412237396695499
 ACS_B08135|US:ST:MN | US:ST:ME | 0.7345206830259712 |  0.5309586339480575 |   2.151810351714706 |     US:ST:MN |  1.5805492092838411
 ACS_B08135|US:ST:MN | US:ST:MI | 0.6939090629600868 |  0.6121818740798265 |  1.6178673461102733 |     US:ST:MN |  1.1226528141331023
 ACS_B08135|US:ST:MN | US:ST:MN |                0.5 |                 1.0 |                 0.0 |     US:ST:MN |                 0.0

```
