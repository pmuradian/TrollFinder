import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    val _ = new TrollFinder()
  }
}

object Math {
  def median(list: List[Int]): Double = {
  val sz = list.size

    if (sz == 0) {
      return 0
    }

    if (sz == 1) {
      return list.last
    }

    if (sz % 2 == 0) {
      val d = sz / 2
      val a =  list.take(d).last.toDouble
      val b = list.take(d + 1).last.toDouble
      return (a + b) / 2
    } else {
      val d = (sz / 2).toDouble
      return list.take(math.ceil(d).toInt).last.toDouble
    }
  }
}

class TrollFinder {
  private val hdfsPath = "hdfs://localhost:8020"

  val sparkConf = new SparkConf().setAppName("TrollFinder").setMaster("local")
  val sc = new SparkContext(sparkConf)

  val arr = List[Integer]()

  var bikct = Seq.fill(5000)(Random.nextInt(1000))
  var bvpuct = Seq.fill(5000)(Random.nextInt(10))

  val items = sc.parallelize(bikct zip bvpuct)//textFile(hdfsPath ++ "/user/azazel/*.csv").cache()

//  items.foreach(x => println(x.toString))

  // Terasort by movieID, get (movieID, [sorted_ratings]), convert to (movieID, median)

//  val movieID_rating = items.map(x => x.replace("(", "").replace(")","").split(",")).map(x => (x(1), x(2).toInt))

//  movieID_rating.foreach( x => println(x))

  val reduced = items.groupByKey()
                      .map(x => (x._1, x._2.toList.sortWith(_ < _)))
                      .map(x => (x._1, Math.median(x._2)))
//
  reduced.foreach(x => println(x).toString)
}