from pyspark.sql import SparkSession
from operator import add
from pyspark import SparkContext
from pyspark.sql import *
import os
import numpy as np
import pandas as pd
os.environ["SPARK_HOME"] = "C:\spark\spark-2.4.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="c:\\winutils"



spark = SparkSession .builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
sc=SparkContext.getOrCreate()

df = spark.read.csv("D:\Drivers\github\Big-Data-Programming\Module2\ICP3-pyspark\\survey.csv",header=True);
df.createOrReplaceTempView("survey")
df.write.csv("D:\Drivers\github\Big-Data-Programming\Module2\ICP3-pyspark\\out3.csv")
df.dropDuplicates()
print("count")
print(df.count())
df1 = df.limit(5)
df2 = df.limit(10)
unionDf = df1.unionAll(df2)
print("union")
unionDf.orderBy('Country').show()
print("GroupBy country with Age avg")
df.groupby('Country').agg({'Age': 'mean'}).show()
joined_df = df1.join(df2, df1.Country == df2.Country)
print("join on country")
joined_df.show()
df.createOrReplaceTempView("survey")
sqlDF = spark.sql("SELECT max(`Age`) FROM survey")
sqlDF.show()
sqlDF = spark.sql("SELECT avg(`Age`) FROM survey")
sqlDF.show()
sqlDF = spark.sql("SELECT min(`Age`) FROM survey")
sqlDF.show()
print("13th record")
df13=df.take(13)
print(df13[-1])


