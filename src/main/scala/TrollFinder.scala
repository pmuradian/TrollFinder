import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    val _ = new TrollFinder()
  }
}

// Helper math function for median calculation
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

// Test date generator
// a is for userID (0, 199)
// b is for movieID (0, 999)
// c is for rating (1, 10)
// potential spammers have userID >= 100
class TestDataGenerator() {

  def generate(): Seq[(Int, (Int, Int))] = {
    var a = Seq.fill(5000)(Random.nextInt(100))
    var b = Seq.fill(5000)(Random.nextInt(1000))
    var c = Seq.fill(5000)(Random.nextInt(9) + 1)

    a = a.union(Seq.fill(250)(Random.nextInt(100) + 100))
    b = b.union(Seq.fill(250)(Random.nextInt(1000)))
    c = c.union(Seq.fill(170)(1)).union(Seq.fill(80)(10))

    return a zip (b zip c)
  }
}

// Main class
class TrollFinder {
  private val hdfsPath = "hdfs://localhost:8020"

  private val sparkConf = new SparkConf().setAppName("TrollFinder").setMaster("local")
  private val sc = new SparkContext(sparkConf)

//  // Generate local test input
//  private val ratings = sc.parallelize(new TestDataGenerator().generate()).cache()

  // Read input from HDFS
  val ratings = sc.textFile(hdfsPath + "/user/azazel/*.csv")
                      .map(x => x.replace("(", "").replace(")","").split(","))
                      .map(x => (x(0), (x(1), x(2).toInt))).cache()

  private val movieID_rating = ratings.map(x => (x._2._1, x._2._2)).cache()
  private val userID_rating = ratings.map(x => (x._1, x._2._2)).cache()

  private val movieID_median = movieID_rating.groupByKey()
                                      .map(x => (x._1, x._2.toList.sortWith(_ < _)))
                                      .map(x => (x._1, Math.median(x._2))).cache()

  private val potentialSpammers = userID_rating.groupByKey()
                                        .map(x => (x._1, x._2.size, x._2.toList.sum))
                                        .filter(x => (x._2 == x._3) || (10 * x._2 == x._3))
                                        .map(x => (x._1, 0)).cache()

  private val spammerID_movieID_rating_median = potentialSpammers.join(ratings)
                                                          .map(x => (x._2._2._1, (x._1, x._2._2._2)))
                                                          .join(movieID_median).cache()

  private val spammers = spammerID_movieID_rating_median.filter(x => math.abs(x._2._2 - x._2._1._2) >= 5)
                                                .map(x => x._2._1._1)
                                                .distinct()

  println("---------- Found spammers -----------")
  spammers.foreach(println)
}