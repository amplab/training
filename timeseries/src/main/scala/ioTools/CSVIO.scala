package main.scala.ioTools

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.{Column, DataFrame, SQLContext, Row}
import org.apache.spark.sql.DataFrame

/**
 * Created by Francois Belletti on 6/22/15.
 */
object CsvIo {

  def loadFromCsv(filePath: String, sqlContext: SQLContext, header: Boolean): DataFrame ={
    val headerString = if (header) "true" else "false"
    sqlContext
      .load("com.databricks.spark.csv",
        Map("path" -> filePath,
          "header" -> headerString
        )
      )
  }


}
