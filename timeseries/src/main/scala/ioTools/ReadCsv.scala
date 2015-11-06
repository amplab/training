package main.scala.ioTools

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import main.scala.overlapping.timeSeries._

import scala.io
import scala.util.Try

/**
 * Created by Francois Belletti on 9/22/15.
 */
object ReadCsv {

  /**
   * Parse a row and convert timestamp.
   *
   * @param row
   * @param indexCol Index of the column holding the timestamp.
   * @param sep
   * @param dateTimeFormat
   * @return
   */
  def parseRow(
      row: String,
      indexCol: Int,
      sep: String,
      dateTimeFormat: String,
      replaceNA: Double = 0.0): Option[(TSInstant, DenseVector[Double])] ={

    val splitRow = row.split(sep).map(_.trim)
    val dataColumns = splitRow.indices.filter(i => i != indexCol).toArray

    def converToDouble(s: String): Double = {
      Try{s.toDouble}.toOption.getOrElse(replaceNA)
    }

    Try {
      val timestamp = DateTime.parse(splitRow(indexCol), DateTimeFormat.forPattern(dateTimeFormat))
      val rowData = DenseVector(dataColumns.map(i => converToDouble(splitRow(i))))
      return Some((new TSInstant(timestamp), rowData))
    }.toOption

  }

  /**
   * Parse a csv file and convert it to a RDD.
   * All rows for which parsing fails will be skipped.
   *
   * @param sc
   * @param filePath
   * @param indexCol
   * @param dateTimeFormat
   * @param header
   * @param sep
   * @return
   */
  def TS(
      filePath: String,
      indexCol: Int = 0,
      dateTimeFormat: String = "yyyy-MM-dd HH:mm:ss",
      header: Boolean = true,
      sep: String = ",",
      replaceNA : Double = 0.0)
      (implicit sc: SparkContext): (RDD[(TSInstant, DenseVector[Double])], Int, Long) = {

    val data = sc.textFile(filePath)

    val nDims = data.take(1)(0).split(sep).length - 1

    val rdd = data
      .map(parseRow(_, indexCol, sep, dateTimeFormat))
      .filter(_.nonEmpty)
      .map(_.get)
      .filter(_._2.length == nDims)

    val nSamples = rdd.count

    (rdd, nDims, nSamples)

  }

  /**
   * For geo-temporal indexing
   * @param row
   * @param timeIndexCol
   * @param lonIndexCol
   * @param latIndexCol
   * @param sep
   * @param dateTimeFormat
   * @return
   */
  def parseRow(
      row: String,
      timeIndexCol: Int,
      lonIndexCol: Int,
      latIndexCol: Int,
      sep: String,
      dateTimeFormat: String): Option[((TSInstant, Double, Double), DenseVector[Double])] ={

    val splitRow = row.split(sep).map(_.trim)
    val dataColumns = splitRow.indices.filter(i => (i != timeIndexCol) && (i != lonIndexCol) && (i != latIndexCol)).toArray

    Try {
      val timestamp = DateTime.parse(splitRow(timeIndexCol), DateTimeFormat.forPattern(dateTimeFormat))
      val lon = splitRow(lonIndexCol).toDouble
      val lat = splitRow(latIndexCol).toDouble
      val rowData = DenseVector(dataColumns.map(i => splitRow(i).toDouble))
      return Some(((new TSInstant(timestamp), lon, lat), rowData))
    }.toOption

  }

  /**
   * For geo-temporal indexing.
   * @param sc
   * @param filePath
   * @param timeIndexCol
   * @param lonIndexCol
   * @param latIndexCol
   * @param dateTimeFormat
   * @param header
   * @param sep
   * @return
   */
  def geoTS(
      filePath: String,
      timeIndexCol: Int = 0,
      lonIndexCol: Int = 1,
      latIndexCol: Int = 2,
      dateTimeFormat: String = "yyyy-MM-dd HH:mm:ss",
      header: Boolean = true,
      sep: String = ",")
      (implicit sc: SparkContext): (RDD[((TSInstant, Double, Double), DenseVector[Double])], Int, Long) = {

    val data = sc.textFile(filePath)

    val rdd = data
      .map(parseRow(_, timeIndexCol, lonIndexCol, latIndexCol, sep, dateTimeFormat))
      .filter(_.nonEmpty)
      .map(_.get)

    val nSamples = rdd.count

    val temp = rdd.takeSample(true, 1, 42)

    val nDims = rdd.takeSample(true, 1, 42)(0)._2.length

    (rdd, nDims, nSamples)

  }

}
