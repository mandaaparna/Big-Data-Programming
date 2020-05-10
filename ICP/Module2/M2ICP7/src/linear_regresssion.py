from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
spark = SparkSession.builder.appName("ICP7").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("../datasets/imports-85.data")
data = data.withColumnRenamed("symboling", "label").select("label", "length", "width", "height")
assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)
data.show()
lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
model = lr.fit(data)
print("Coefficients: %s" % str(model.coefficients))
print("Intercept: %s" % str(model.intercept))
trainingSummary = model.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)
