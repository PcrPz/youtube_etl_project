### End To End Automated Data Pipeline(Youtube Data Analysis)
## Technology Stack
Python,Postgres,Docker,Prefect
## Workflow
the workflow is divided into 3 parts of ETL(Extract,Load,Transfrom)
1.Extract: Extract Data from kaggle website [link here](https://www.kaggle.com/datasets/datasnaek/youtube-new) and stored in a folder 
2.Transfrom: in this part we have folder in part one we divided into 3 file Asia,Europe,North America(dimesion_table) the data undergoes a cleaning process, addressing issues like handling null values, remove empty data. Once the dimension tables are created, a fact table is constructed as a bridge table, linking all the dimension tables using surrogate keys. In addition to these keys, new columns are introduced to provide insights into the most interacted videos in each region. These columns include:
 - eu_interaction_rate
 - na_interaction_rate
 - as_interaction_rate
These columns represent the video interaction rates for each region, calculated based on views, likes, and dislikes,comment count.
3.Load:we have clean dataset are the loaded into postgres(docker) connect from python 
The entire process is automated pipeline using Prefect

## Snapshot of Result Data
![Result_Data](Result_Data.png)