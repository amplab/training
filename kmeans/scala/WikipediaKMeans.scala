import spark.SparkContext
import spark.SparkContext._
import spark.util.Vector

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.Random
import scala.io.Source

object WikipediaKMeans {
  def parseVector(line: String): Vector = {
      return new Vector(line.split(',').map(_.toDouble))
  }

  // Add any new functions you need here

  def main(args: Array[String]) {
    Logger.getLogger("spark").setLevel(Level.WARN)
    val sc = new SparkContext("local[6]", "WikipediaKMeans")

    val K = 10
    val convergeDist = 1e-6

    val data = sc.textFile("/dev/ampcamp/imdb_data/wikistats_featurized").map(
            t => (t.split("#")(0), parseVector(t.split("#")(1)))).cache()


    // Your code goes here
    sc.stop();
    System.exit(0)
  }
}
