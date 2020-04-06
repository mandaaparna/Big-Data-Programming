import org.apache.spark._

object wordcount {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils" )
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("D:\\Drivers\\github\\Big-Data-Programming\\Module2\\ICP1\\input2.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(","))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("output13")
  }
}
