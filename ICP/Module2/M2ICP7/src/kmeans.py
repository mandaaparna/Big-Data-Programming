from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
spark = SparkSession.builder.appName("ICP 14").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load(
    "../datasets/dataset_diabetes/diabetic_data.csv")
data = data.select("admission_type_id", "discharge_disposition_id", "admission_source_id", "time_in_hospital",
                   "num_lab_procedures")
assembler = VectorAssembler(inputCols=data.columns, outputCol="features")
data = assembler.transform(data)
data.show()
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(data)
predictions = model.transform(data)
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
