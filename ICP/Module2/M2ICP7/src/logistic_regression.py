from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
spark = SparkSession.builder.appName("ICP 7").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load(
    "../datasets/imports-85.data")
data = data.withColumn("label", when(col("num-of-doors") == "four", 1).otherwise(0)).select("label", "length", "width",
                                                                                            "height")
assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)
data.show()
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
model = lr.fit(data)
print("Coefficients: " + str(model.coefficients))
print("Intercept: " + str(model.intercept))
mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")
mlr_model = mlr.fit(data)
print("Multinomial coefficients: " + str(mlr_model.coefficientMatrix))
print("Multinomial intercepts: " + str(mlr_model.interceptVector))

