# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import countDistinct, count
from pyspark.sql.functions import round,ceil


# COMMAND ----------

service_credential = dbutils.secrets.get(scope="key-vault-scope1",key="secretkey")

# COMMAND ----------




configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "8a00d73d-c9d6-4a27-823f-8fe22afc957b" ,
"fs.azure.account.oauth2.client.secret": service_credential,
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d1c47a41-82ac-441d-91bb-8035c959c41b/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@olympicdatastorage.dfs.core.windows.net", # container@storage_account_name
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolympic"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/teams.csv")


# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

# DBTITLE 1,Analysis On Athletes data
athletes.show()

# COMMAND ----------

# DBTITLE 1,Countries with highest number of participating athletes

athletes.groupBy("Country").count().orderBy("count",ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Most Participated Sports
athletes.groupBy("Discipline").count().orderBy("count",ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Least Participated Sports
athletes.groupBy("Discipline").count().orderBy("count",ascending=True).show()

# COMMAND ----------

# DBTITLE 1,Athletes who participate in multiple sports or discipline 
multi_sport_athletes = athletes.groupBy("PersonName").agg(countDistinct("Discipline").alias("Sport_Count")).filter("Sport_Count > 1")
multi_sport_athletes.show()

# COMMAND ----------

filtered_athletes = athletes.filter((athletes["Country"] == "United States of America") & (athletes["Discipline"] == "Swimming"))

filtered_athletes.show()

# COMMAND ----------

# DBTITLE 1,Analysis on teams data 
teams.show()

# COMMAND ----------

# DBTITLE 1,How many teams were participated from each country 

teams.groupBy("TeamName","Discipline").count().orderBy("count",ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Top discipline or sports with highest number of teams
teams.groupBy("Discipline").count().orderBy("count",ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Top disciplines in which Women participated 
Women= teams.filter(teams["Event"]=="Women").groupBy("Discipline").count().orderBy("count",ascending=False)

Women.show()

# COMMAND ----------

# DBTITLE 1,Top disciplines in which Men participated 
Men= teams.filter(teams["Event"]=="Men").groupBy("Discipline").count().orderBy("count",ascending=False)

Men.show()

# COMMAND ----------

# DBTITLE 1,Disciplines or sports in which Women's Team participated 

womens_team= teams.filter(teams["Event"]=="Women's Team").groupBy("Discipline").count().orderBy("count",ascending=False)

womens_team.show()


# COMMAND ----------

# DBTITLE 1,Analysis on Coaches data
coaches.show()

# COMMAND ----------

# DBTITLE 1,Countries having highest number of coaches 
#coaches.filter(col("Event").isNull()).groupBy("Country").count().orderBy("count",ascending=False).show()

coaches.filter(col("Event")!="NULL").groupBy("Country").count().orderBy("count",ascending=False).show()


# COMMAND ----------

coaches.groupBy("Name").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Distribution of coaches across different countries and sports or discipline 
coach_distribution = coaches.groupBy("Country", "Discipline").count().orderBy("count", ascending=False)
coach_distribution.show()

# COMMAND ----------

# DBTITLE 1,How many Football Coaches by country

football_coaches1=coaches.filter((col("Discipline")=="Football") & (col("Event")!="NULL"))

football_coaches2=football_coaches1.groupBy("Country").count().orderBy("count",ascending=False)

football_coaches2.show()

# COMMAND ----------

# DBTITLE 1,Analysis on Gender Entries data  
entriesgender.show()

# COMMAND ----------

# DBTITLE 1,Average number of entries by gender for each sport or discipline
average_entries_by_gender = entriesgender.withColumn('Avg_Female', (entriesgender['Female'] / entriesgender['Total'])*100).withColumn('Avg_Male', (entriesgender['Male'] / entriesgender['Total'])*100)

average_entries_by_gender.show()

# COMMAND ----------

# DBTITLE 1,Most participated discipline by both gender
both_gender=entriesgender.sort("Total",ascending=False)

both_gender.show()

# COMMAND ----------

# DBTITLE 1,Least participated discipline by both gender
both_gender=entriesgender.sort("Total",ascending=True)

both_gender.show()

# COMMAND ----------

# DBTITLE 1,Analysis on Medals data 
medals.show()

# COMMAND ----------

# DBTITLE 1,Total medals won by each country
from pyspark.sql import functions as F

total_medals_by_country = medals.groupBy("Team_Country").agg(F.sum("Gold").alias("Total_Gold"),F.sum("Silver").alias("Total_Silver"),F.sum("Bronze").alias("Total_Bronze")
).orderBy("Total_Gold", ascending=False)

total_medals_by_country.show()

# COMMAND ----------

# DBTITLE 1,Top countries with the highest number of gold medals
# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------

# DBTITLE 1,Top countries with the highest number of silver medals
top_silver_medal_countries= medals.orderBy("Silver",ascending=False).select("Team_country","Silver")
top_silver_medal_countries.show()

# COMMAND ----------

# DBTITLE 1,Top countries with the highest number of bronze medals
top_bronze_medal_countries= medals.orderBy("Bronze",ascending=False).select("Team_country","Bronze")
top_bronze_medal_countries.show()

# COMMAND ----------

# DBTITLE 1,Top countries with highest number of total medals 
top_total_medal_countries= medals.sort("Total",ascending=False).select("Team_country","Total")
top_total_medal_countries.show()

# COMMAND ----------


athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/athletes")
     
coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/coaches")

entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")

medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/medals")

teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/teams")
     

