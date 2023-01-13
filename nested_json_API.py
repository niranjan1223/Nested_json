# Databricks notebook source
# nested json 1

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark =SparkSession.builder.master("local[*]").appName("json").getOrCreate()


# COMMAND ----------

# data = {
# 	"id": "0001",
# 	"type": "donut",
# 	"name": "Cake",
# 	"image":
# 		{
# 			"url": "images/0001.jpg",
# 			"width": 200,
# 			"height": 200
# 		},
# 	"thumbnail":
# 		{
# 			"url": "images/thumbnails/0001.jpg",
# 			"width": 32,
# 			"height": 32
# 		}
# }

# COMMAND ----------

# imageSchema = StructType([
#     StructField("url", StringType(), True),
#     StructField("width", IntegerType(), True),
#     StructField("height", IntegerType(), True)
# ])

# COMMAND ----------

# thumbnailSchema = StructType([
#     StructField("url", StringType(), True),
#     StructField("width", IntegerType(), True),
#     StructField("height", IntegerType(), True)
# ])

# COMMAND ----------

# rootSchema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("type", StringType(), True),
#     StructField("name", StringType(), True),
#     StructField("image", StructType(imageSchema), True),
#     StructField("thumbnail", StructType(thumbnailSchema), True),
# ])

# COMMAND ----------

# df= spark.createDataFrame(data=data,schema=rootSchema)


# COMMAND ----------



# COMMAND ----------

# Nested Json 2

#reading nested json file
df = spark.read.option('multiline','True').json("dbfs:/FileStore/tables/new_9.json")
df.show()

# COMMAND ----------


