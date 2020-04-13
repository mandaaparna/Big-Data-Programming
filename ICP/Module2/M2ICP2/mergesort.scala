import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.expressions.UserDefinedFunction
//import org.apache.spark.sql.functions.udf

object mergesort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //org.apache.spark.sql.functions.udf.register("convert",convert _)
    def mergeSort(xs: List[Int]): List[Int] = {
      //val data = sc.parallelize(list)
      val n = xs.length / 2
      if (n == 0) xs
      else {
        def merge(xs: List[Int], ys: List[Int]): List[Int] =
          (xs, ys) match {
            case(Nil, ys) => ys
            case(xs, Nil) => xs
            case(x :: xs1, y :: ys1) =>
              if (x < y) x::merge(xs1, ys)
              else y :: merge(xs, ys1)
          }
        val (left, right) = xs splitAt(n)
        merge(mergeSort(left), mergeSort(right))

      }
    }
    val list = List(5,8,6,3,1,2,4,7)
    val result = sc.parallelize(Seq(list)).map(mergeSort)
    result.foreach(println)
    println(result)

  }
}