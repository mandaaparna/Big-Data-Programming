from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext("local[2]", "NetworkWordCount")
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org").setLevel( logger.Level.OFF )
logger.LogManager.getLogger("akka").setLevel( logger.Level.OFF )
ssc=StreamingContext(sc,3)
lines = ssc.textFileStream('log')
words=lines.flatMap(lambda line:line.split(' '))
wordtup=words.map(lambda word:(word,1))
wordcount=wordtup.reduceByKey(lambda x,y:x+y)
wordcount.pprint()
ssc.start()
ssc.awaitTermination()