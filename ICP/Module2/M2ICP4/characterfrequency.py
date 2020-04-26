

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "NetworkCharacterFreqency")
ssc = StreamingContext(sc, 5)

lines = ssc.socketTextStream("localhost", 7000)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (len(word), word))
wordCounts = pairs.reduceByKey(lambda x, y: x + ',' + y)

wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
