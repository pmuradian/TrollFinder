import org.apache.spark.rdd.RDD
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

  var arbabik = Seq.fill(5000)(Random.nextInt(100))
  var bikct = Seq.fill(5000)(Random.nextInt(1000))
  var bvpuct = Seq.fill(5000)(Random.nextInt(9) + 1)
  arbabik = arbabik.union(Seq.fill(250)(Random.nextInt(100) + 100))
  bikct = bikct.union(Seq.fill(250)(Random.nextInt(1000)))
  bvpuct = bvpuct.union(Seq.fill(170)(1))
  bvpuct = bvpuct.union(Seq.fill(80)(10))

  val ratings = sc.parallelize(arbabik zip (bikct zip bvpuct)).cache()//textFile(hdfsPath ++ "/user/azazel/*.csv").cache()
//  val userID_ratings = sc.parallelize(arbabik zip items)

//  items.foreach(x => println(x.toString))

  // Terasort by movieID, get (movieID, [sorted_ratings]), convert to (movieID, median)

//  val movieID_rating = items.map(x => x.replace("(", "").replace(")","").split(",")).map(x => (x(1), x(2).toInt))

//  movieID_rating.foreach( x => println(x))

  val ratingTuples = ratings.map(x => (x._2._1, x._2._2))
  val userID_ratings = ratings.map(x => (x._1, x._2._2))

  val movieID_median = ratingTuples.groupByKey()
                      .map(x => (x._1, x._2.toList.sortWith(_ < _)))
                      .map(x => (x._1, Math.median(x._2)))

//  val userID_movieID_median = ratings.map(x => (x._2._1, x._1)).join(movieID_median).cache()
//
//  userID_movieID_median.map(x => x)
//
  val potential_spammers = userID_ratings.groupByKey()
                .map(x => (x._1, x._2.size, x._2.toList.sum))
                .filter(x => (x._2 == x._3) || (10 * x._2 == x._3))
                .map(x => (x._1, 0))

  val collected = potential_spammers.join(ratings).map(x => (x._2._2._1, (x._1, x._2._2._2))).join(movieID_median)
  val spammers = collected.filter(x => x._2._2 - x._2._1._2 >= 5 || x._2._2 - x._2._1._2 <= -5).map(x => x._2._1._1).distinct()
  println("---------- Found spammers -----------")
  spammers.foreach(println)
}