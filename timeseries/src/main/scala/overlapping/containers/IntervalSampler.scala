package main.scala.overlapping.containers

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.math.Ordering
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/6/15.
 *
 * This class samples out from an RDD and computes the approximate intervals
 * that should be used for an even partitioning.
 *
 */
object IntervalSampler{

  /**
   * Sample out a small data set from the complete data and
   * devise intervals so that about the same number of data points
   * belong to each interval.
   *
   * @param nIntervals Number of intervals desired
   * @param sampleSize Number of samples used to decide interval lengths
   * @param sourceRDD Data
   * @param count If n samples is already known, avoids recomputation
   * @param withReplacement Sample with or without replacement
   * @return An array of intervals (begin, end)
   */
  def sampleAndComputeIntervals[IndexT <: Ordered[IndexT]: ClassTag, ValueT: ClassTag](
      nIntervals: Int,
      sampleSize: Int,
      sourceRDD: RDD[(IndexT, ValueT)],
      count: Option[Long],
      withReplacement: Boolean = false): Array[(IndexT, IndexT)] = {

    val fraction = sampleSize.toDouble / count.getOrElse(sourceRDD.count()).toDouble

    val stride = sampleSize / nIntervals

    val sortedKeys: Array[IndexT]  = sourceRDD
      .sample(withReplacement, fraction)
      .map(_._1)
      .sortBy(x => x)
      .collect()
      .sliding(1, stride)
      .map(_.head)
      .toArray

    sortedKeys.zip(sortedKeys.drop(1))
  }

}
