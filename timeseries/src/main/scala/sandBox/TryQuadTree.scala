package main.scala.sandBox

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.plot._
import main.scala.ioTools.ReadCsv
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import main.scala.overlapping._
import main.scala.overlapping.spatial.QuadTree
import main.scala.overlapping.timeSeries._

object TryQuadTree {

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

  def main(args: Array[String]): Unit = {


    val nPartitions = 8

    //implicit val config = TSConfig(deltaTMillis, d, N, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    val MAX_LAT = 40.85
    val MIN_LAT = 40.66

    val MAX_LON = -73.90
    val MIN_LON = -74.05

    val nX = 100
    val nY = 100
    val deltaX = (MAX_LON - MIN_LON) / nX.toDouble
    val deltaY = (MAX_LAT - MIN_LAT) / nY.toDouble

    val inSampleFilePath = "/users/cusgadmin/traffic_data/new_york_taxi_data/earnings_table_1_short.csv"

    val (inSampleData_, d, nSamples) = ReadCsv.geoTS(inSampleFilePath)

    def hasher(x: Double, y: Double): (Int, Int) = {
      (((x - MIN_LON) / deltaX).toInt, ((y - MIN_LAT) / deltaY).toInt)
    }

    val inSampleData = inSampleData_
      .filter({case ((_, x: Double, y: Double), _) => (x >= MIN_LON) && (x <= MAX_LON) && (y >= MIN_LAT) && (y <= MAX_LAT)})

    val countMap = inSampleData
      .map({case ((_, x, y), _) => (hasher(x, y), 1L)})
      .reduceByKey(_ + _)
      .collect()

    val countMatrix = DenseMatrix.zeros[Double](nX, nY)
    for(((i, j), c) <- countMap){
      countMatrix(i, j) = c
    }

    val qt = new QuadTree(MIN_LON, MIN_LAT, MAX_LON, MAX_LAT, countMatrix, 50000L)

    val f1 = Figure()
    f1.subplot(0) += image(countMatrix)

    val leafLocations = qt.getLeafLocations()

    val leafLongitudes = DenseVector(leafLocations.map(_._1))
    val leafLatitudes  = DenseVector(leafLocations.map(_._2))

    val f2 = Figure()
    val p = f2.subplot(0)
    p += plot(leafLongitudes, leafLatitudes, '.')

    //inSampleData_.collect().foreach(println)

  }
}