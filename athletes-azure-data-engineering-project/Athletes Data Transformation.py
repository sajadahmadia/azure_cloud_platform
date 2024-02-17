# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "b1f1a871-0799-4df2-a4c7-7488b11561d0",
"fs.azure.account.oauth2.client.secret": "1P58Q~Fz7NYjx4EtxZR8N.PnxwQLP1zkyPy73c6v",
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d01a706c-efe5-446b-a19e-311a19b2c970/oauth2/token"}


dbutils.fs.mount(
source = "abfss://athletes-blob-data@athletesstorage.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/athletes.csv")
athletes.show()

# COMMAND ----------

coaches = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/teams.csv")


# COMMAND ----------

coaches.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

#changing the datatype of the Famale column
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals = medals.withColumn("Rank",col("Rank").cast(IntegerType()))\
    .withColumn("Gold",col("Gold").cast(IntegerType()))\
    .withColumn("Silver",col("Silver").cast(IntegerType()))\
    .withColumn("Bronze",col("Bronze").cast(IntegerType()))\
    .withColumn("Rank by Total",col("Rank by Total").cast(IntegerType()))   

medals.printSchema()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()


# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

from pyspark.sql.functions import format_number

average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', format_number(entriesgender['Female'] / entriesgender['Total'], 2)
).withColumn(
    'Avg_Male', format_number(entriesgender['Male'] / entriesgender['Total'], 2)
)
average_entries_by_gender.show()


# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/athletes")

# COMMAND ----------

coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/teams")
