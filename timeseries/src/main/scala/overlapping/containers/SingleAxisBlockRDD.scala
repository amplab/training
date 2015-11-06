package main.scala.overlapping.containers

import breeze.linalg.{min, DenseVector}
import breeze.numerics.{log, sqrt}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import main.scala.overlapping.timeSeries.TSInstant

import scala.math.Ordering
import scala.reflect.ClassTag

/**
 * This object is used to transform an RDD of raw unsorted data into
 * an RDD of key value pairs where keys are partition indices and values
 * are SingleAxisBlocks inside which the data is sorted.
 */
object SingleAxisBlockRDD {

  /**
   * This will devise approximatively balanced intervals to partition the raw data along.
   * Partitions will be created, overlaps materialized and the data within each block will be sorted.
   *
   * @param padding Backward and forward padding the main.scala.overlapping partitioning will provide.
   * @param nPartitions Number of partitions desired.
   * @param recordRDD Input data.
   * @param signedDistance Signed distance function between timestamps.
   * @tparam IndexT Timestamp type.
   * @tparam ValueT Data type.
   * @return The resulting main.scala.overlapping block RDD and and array of intervals (begin, end).
   */
  def apply[IndexT <: Ordered[IndexT] : ClassTag, ValueT: ClassTag](
      padding: (Double, Double),
      nPartitions: Int,
      recordRDD: RDD[(IndexT, ValueT)])
      (implicit signedDistance: (IndexT, IndexT) => Double): (RDD[(Int, SingleAxisBlock[IndexT, ValueT])], Array[(IndexT, IndexT)]) = {

    //println("Starting to create main.scala.overlapping block RDD")

    val nSamples = recordRDD.count()

    val intervals = IntervalSampler
      .sampleAndComputeIntervals(
        nPartitions,
        sqrt(nSamples).toInt,
        recordRDD,
        Some(nSamples))

    val replicator = new SingleAxisReplicator[IndexT, ValueT](intervals, signedDistance, padding)
    val partitioner = new BlockIndexPartitioner(intervals.length)

    //println("Replicator and partitioner set up done.")

    (recordRDD
      .flatMap({ case (k, v) => replicator.replicate(k, v) })
      .partitionBy(partitioner)
      .mapPartitionsWithIndex({case (i, x) => ((i, SingleAxisBlock(x.toArray, signedDistance)) :: Nil).toIterator}, true)
    ,intervals)

  }

  /**
   * This is to build an RDD with predefined partitioning intervals.
   * This is useful so that two OverlappingBlock RDDs have
   * corresponding main.scala.overlapping blocks mapped on the same key.
   *
   * @param padding Backward and forward padding in distance metric (usually milli seconds).
   * @param signedDistance The distance to use.
   * @param intervals
   * @param recordRDD
   * @tparam IndexT
   * @tparam ValueT
   * @return
   */
  def fromIntervals[IndexT <: Ordered[IndexT], ValueT: ClassTag](
      padding: (Double, Double),
      intervals: Array[(IndexT, IndexT)],
      recordRDD: RDD[(IndexT, ValueT)])
      (implicit signedDistance: (IndexT, IndexT) => Double): RDD[(Int, SingleAxisBlock[IndexT, ValueT])] = {

    case class KeyValue(k: IndexT, v: ValueT)

    val replicator = new SingleAxisReplicator[IndexT, ValueT](intervals, signedDistance, padding)
    val partitioner = new BlockIndexPartitioner(intervals.length)

    recordRDD
      .flatMap({ case (k, v) => replicator.replicate(k, v) })
      .partitionBy(partitioner)
      .mapPartitionsWithIndex({case (i, x) => ((i, SingleAxisBlock(x.toArray, signedDistance)) :: Nil).toIterator}, true)

  }

  /*
  What follows concerns data spatial partitioning for high dimensional studies

  def splitArray[IndexT <: Ordered[IndexT], ValueT: ClassTag](mainAxisPadding: (Double, Double),
                                                              mainAxisSignedDistance: (IndexT, IndexT) => Double,
                                                              mainAxisNBlocks: Int,
                                                              lateralPartitions: Array[Array[Int]],
                                                              recordRDD: RDD[(IndexT, Array[ValueT])]):
  (RDD[(Int, SingleAxisBlock[IndexT, Array[ValueT]])], Array[(IndexT, IndexT)]) = {

    case class KeyValue(k: IndexT, v: ValueT)
    /*
      Sort the record RDD with respect to time
     */
    implicit val kvOrdering = new Ordering[(IndexT, Array[ValueT])] {
      override def compare(a: (IndexT, Array[ValueT]), b: (IndexT, Array[ValueT])) =
        a._1.compareTo(b._1)
    }

    val nSamples = recordRDD.count()

    val intervals = IntervalSampler
      .sampleAndComputeIntervals(
        mainAxisNBlocks,
        sqrt(nSamples).toInt,
        true,
        recordRDD)
      .map({ case ((k1, v1), (k2, v2)) => (k1, k2) })

    val replicator = new SingleAxisLateralReplicator[IndexT, ValueT](
      intervals,
      mainAxisSignedDistance,
      mainAxisPadding,
      lateralPartitions)
    val partitioner = new BlockIndexPartitioner(intervals.length * lateralPartitions.length)

    (recordRDD
      .flatMap({ case (k, v) => replicator.replicate(k, v) })
      .partitionBy(partitioner)
      .mapPartitionsWithIndex({case (i, x) => ((i, SingleAxisBlock(x.toArray, mainAxisSignedDistance)) :: Nil).toIterator}, true)
      ,intervals)

  }

  def splitDenseVector[IndexT <: Ordered[IndexT], ValueT: ClassTag](mainAxisPadding: (Double, Double),
                                                                    mainAxisSignedDistance: (IndexT, IndexT) => Double,
                                                                    mainAxisNBlocks: Int,
                                                                    lateralPartitions: Array[Array[Int]],
                                                                    recordRDD: RDD[(IndexT, DenseVector[ValueT])]):
  (RDD[(Int, SingleAxisBlock[IndexT, DenseVector[ValueT]])], Array[(IndexT, IndexT)]) = {

    case class KeyValue(k: IndexT, v: ValueT)
    /*
      Sort the record RDD with respect to time
     */
    implicit val kvOrdering = new Ordering[(IndexT, DenseVector[ValueT])] {
      override def compare(a: (IndexT, DenseVector[ValueT]), b: (IndexT, DenseVector[ValueT])) =
        a._1.compareTo(b._1)
    }

    val nSamples = recordRDD.count()

    val intervals = IntervalSampler
      .sampleAndComputeIntervals(
        mainAxisNBlocks,
        sqrt(nSamples).toInt,
        true,
        recordRDD)
      .map({ case ((k1, v1), (k2, v2)) => (k1, k2) })

    val replicator = new SingleAxisDenseLateralReplicator[IndexT, ValueT](
      intervals,
      mainAxisSignedDistance,
      mainAxisPadding,
      lateralPartitions)
    val partitioner = new BlockIndexPartitioner(intervals.length * lateralPartitions.length)

    (recordRDD
      .flatMap({ case (k, v) => replicator.replicate(k, v) })
      .partitionBy(partitioner)
      .mapPartitionsWithIndex({case (i, x) => ((i, SingleAxisBlock(x.toArray, mainAxisSignedDistance)) :: Nil).toIterator}, true)
      ,intervals)

  }

  */

}

