from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
import os
import numpy as np


spark = SparkSession.builder.appName("ICP7").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load(
    "../datasets/adult.data")

data = data.withColumnRenamed("age", "label").select("label", "education-num", "hours-per-week")
data = data.select(data.label.cast("double"), "education-num", "hours-per-week")


assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)
data.show()
training, test = data.select("label", "features").randomSplit([0.6, 0.4])

rf = RandomForestClassifier()
model = rf.fit(training)

predictions = model.transform(test)

evaluator = MulticlassClassificationEvaluator()
accuracy = evaluator.evaluate(predictions)

print("Accuracy:", accuracy)

predictionAndLabels = predictions.select("label", "prediction").rdd
metrics = MulticlassMetrics(predictionAndLabels)
print("Confusion Matrix:", metrics.confusionMatrix())
print("Precision:", metrics.precision())
print("Recall:", metrics.recall())
print("F-measure:", metrics.fMeasure())
