import org.apache.spark.{SparkConf, SparkContext, rdd}

object Main {
  def main(args: Array[String]): Unit = {
    println("bikct ?")
    val trollFinder = new TrollFinder()
  }
}


class TrollFinder {

  private val hdfsPath = "hdfs://localhost:8020"
  private val hdfsName = "fs.defaultFS"

  val sparkConf = new SparkConf().setAppName("TrollFinder").setMaster("local")
  val sc = new SparkContext(sparkConf)

  val items = sc.textFile(hdfsPath ++ "/user/azazel/*.csv")

  items.foreach(x => println(x.toString))
}