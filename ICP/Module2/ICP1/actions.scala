import org.apache.spark._

object actions {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("actions").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val nums = sc.parallelize(Array(1,2,3))

    println(nums.collect())

    println(nums.count())
    println(nums.saveAsTextFile("D:\\Drivers\\github\\Big-Data-Programming\\Module2\\ICP1\\file14.txt"))

  }
}
