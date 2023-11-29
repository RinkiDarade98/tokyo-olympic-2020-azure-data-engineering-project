# tokyo-olympic-2021-azure-data-engineering-project

This is an end to end azure data engineering project. The goal of this project was to create a data pipeline that ingests, transforms, stores, analyses the tokyo olympic 2020 dataset using various azure services.
In this project I have used Azure Data Factory to extract the data from data source (here data source is my github repository) and then load that raw data into Data Lake Gen 2 container.
Then performed some transformations on raw data using Azure Databricks. Again I have loaded the transformed data back to transformed Data Lake Storage. And analysed the data using SQL on Azure Synapse Analytics.

Below image shows the data pipeline which is created to load the data from the data source. 

![olympic_datapipeline](https://github.com/RinkiDarade98/tokyo-olympic-2021-azure-data-engineering-project/assets/129477415/aee846a6-9fb0-4b51-b844-bbd6c024e29d)
