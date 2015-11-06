package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.{DenseVector, sum}
import breeze.stats.distributions.Uniform
import main.scala.overlapping.IntervalSize
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}


class TestSingleAxisBlock extends FlatSpec with Matchers{

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

  val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
  val sc    = new SparkContext(conf)


  "A SingleAxisBlocks" should " properly map elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val rawTS = Surrogate.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (overlappingRDD, _) = SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val nonOverlappingSeqs = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => v_.map(_ * 2.0)}))
      .mapValues(_.toArray)

    val transformedData = nonOverlappingSeqs
      .collect()
      .map(_._2.toList)
      .foldRight(List[(TSInstant, DenseVector[Double])]())(_ ::: _)
      .toArray

    val originalData = rawTS.collect

    for(((ts1, data1), (ts2, data2)) <- transformedData.zip(originalData)){
      ts1 should be (ts2)
      for(j <- 0 until data1.size){
        data1(j) should be (data2(j) * 2.0)
      }
    }

  }

  it should "conserve the sum of its elements when partitioning" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = Surrogate.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => sum(v_)}).reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2._2)
      .reduce(_ + _)

    val directSum = rawTS.map(x => sum(x._2)).reduce(_ + _)

    partitionedSum should be (directSum +- 0.000001)

  }

  it should " return the sum of samples when data is filled with 1.0 value" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => sum(v_)}).reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2._2)
      .reduce(_ + _)

    partitionedSum should be (nSamples.toDouble * nColumns.toDouble +- 0.000001)

  }

  it should "conserve the sum of its elements when partitioning when deltaT != 1" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 3L
    val nPartitions   = 8

    val rawTS = Surrogate.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => sum(v_)}).reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2._2)
      .reduce(_ + _)

    val directSum = rawTS.map(x => sum(x._2)).reduce(_ + _)

    partitionedSum should be (directSum +- 0.000001)

  }

  it should " return the sum of samples when data is filled with 1.0 value and deltaT != 1" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => sum(v_)}).reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2._2)
      .reduce(_ + _)

    partitionedSum should be (nSamples.toDouble * nColumns.toDouble +- 0.000001)

  }

  it should "return a proper overlap aware iterator" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val nonOverlappingSeqs = overlappingRDD
      .mapValues(_.toArray)

    val transformedData = nonOverlappingSeqs
      .collect()
      .map(_._2.toList)
      .foldRight(List[(TSInstant, DenseVector[Double])]())(_ ::: _)
      .toArray

    val originalData = rawTS.collect

    for(((ts1, data1), (ts2, data2)) <- transformedData.zip(originalData)){
      ts1 should be (ts2)
      for(j <- 0 until data1.size){
        data1(j) should be (data2(j))
      }
    }

  }

  it should " properly reduce elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val reduceFromOverlappingData = overlappingRDD
      .mapValues(v => v.reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2)
      .reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)})

    val reduceFromNonOverlappingData = rawTS
      .reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)})

    for(j <- 0 until reduceFromOverlappingData._2.length){
      reduceFromOverlappingData._2(j) should be (reduceFromNonOverlappingData._2(j))
    }

  }

  it should " properly fold elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val zeroArray   = DenseVector.zeros[Double](nColumns)
    val zeroSecond  = TSInstant(new DateTime(0L))

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val foldFromOverlappingData = overlappingRDD
      .mapValues(v => v.fold((zeroSecond, zeroArray))(
        {case (k, v) => (k, v)},
        {case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2)
      .reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)})

    val foldFromNonOverlappingData = rawTS
      .fold((zeroSecond, zeroArray))(
        {case ((k1, v1), (k2, v2)) => (k1, v1 + v2)})

    for(j <- 0 until foldFromOverlappingData._2.length){
      foldFromOverlappingData._2(j) should be (foldFromNonOverlappingData._2(j))
    }

  }

  it should " properly slide fold elements" in {

    val nColumns      = 10
    val nSamples      = 10000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val zeroArray   = Array.fill(nColumns)(0.0)
    val zeroSecond  = TSInstant(new DateTime(0L))

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    def sumArray(data: Array[(TSInstant, DenseVector[Double])]): Double = {
      data.map(x => sum(x._2)).sum
    }

    val foldFromOverlappingData = overlappingRDD
      .mapValues(v => v.slidingFold(Array(IntervalSize(1.0, 1.0)))(sumArray, 0.0, (x: Double, y: Double) => x + y))
      .map(_._2)
      .reduce(_ + _)

    foldFromOverlappingData should be ((3 * (nSamples - 2) + 2 * 2) * nColumns)

  }

  it should " properly slide fold elements with external targets" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val zeroArray   = Array.fill(nColumns)(0.0)
    val zeroSecond  = TSInstant(new DateTime(0L))

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    def sumArray(data: Array[(TSInstant, DenseVector[Double])]): Double = {
      data.map(x => sum(x._2)).sum
    }

    val foldFromOverlappingData = overlappingRDD
      .mapValues(v => v.slidingFold(Array(IntervalSize(1.0, 1.0)),
                                    v.locations.slice(v.firstValidIndex, v.lastValidIndex + 1))
                                    (sumArray, 0.0, (x: Double, y: Double) => x + y))
      .map(_._2)
      .reduce(_ + _)

    foldFromOverlappingData should be ((3 * (nSamples - 2) + 2 * 2) * nColumns)

  }

  it should " properly count elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val overlappingCount = overlappingRDD
      .mapValues(_.count)
      .map(_._2)
      .reduce(_ + _)

    overlappingCount should be (nSamples)

  }

  it should " properly filter elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val filteredOverlappingRDDCount = overlappingRDD
      .mapValues(v => v.filter({case (k, _) => k.timestamp.getMillis % 4 == 0}).count)
      .map(_._2)
      .reduce(_ + _)

    filteredOverlappingRDDCount should be (nSamples / 4)

  }

  it should " provide a proper natural sliding operator." in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 2L
    val nPartitions   = 8

    val rawTS = Surrogate.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val flattenedRawTS = rawTS
      .collect()
      .foldRight(List[(TSInstant, DenseVector[Double])]())(_ :: _)
      .toArray

    val slideFromRawTS = flattenedRawTS.sliding(11).toArray

    val intervalSize = IntervalSize(10, 10)

    val slidingRDD = overlappingRDD
      .mapValues(_.sliding(Array(intervalSize))(x => x).toArray)
      .map(_._2)
      .map(_.map(_._2))
      .collect()
      .map(_.toList)
      .foldRight(List[Array[(TSInstant, DenseVector[Double])]]())(_ ::: _)
      .filter(_.length == 11)
      .toArray

    println("Done")

    for((data1, data2) <- slideFromRawTS.zip(slidingRDD)){
      data1.length should be (data2.length)
      for(((ts1, values1), (ts2, values2)) <- data1.zip(data2)) {
        ts1.timestamp.getMillis should be(ts2.timestamp.getMillis)
        for(j <- 0 until values1.size){
          values1(j) should be(values2(j))
        }
      }
    }
  }

  it should " provide a proper external sliding operator." in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 2L
    val nPartitions   = 8

    val rawTS = Surrogate.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (overlappingRDD, _) = SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val flattenedRawTS = rawTS
      .collect()
      .foldRight(List[(TSInstant, DenseVector[Double])]())(_ :: _)
      .toArray

    val slideFromRawTS = flattenedRawTS.sliding(11).toArray

    val intervalSize = IntervalSize(10, 10)

    val slidingRDD = overlappingRDD
      .mapValues(v => v.sliding(Array(intervalSize),
                                v.locations.slice(v.firstValidIndex, v.lastValidIndex + 1))
                                (x => x)
                                .toArray)
      .map(_._2)
      .map(_.map(_._2))
      .collect()
      .map(_.toList)
      .foldRight(List[Array[(TSInstant, DenseVector[Double])]]())(_ ::: _)
      .filter(_.length == 11)
      .toArray

    slideFromRawTS.length should be(slidingRDD.length)

    for((data1, data2) <- slideFromRawTS.zip(slidingRDD)){
      data1.length should be (data2.length)
      for(((ts1, values1), (ts2, values2)) <- data1.zip(data2)) {
        ts1.timestamp.getMillis should be(ts2.timestamp.getMillis)
        for(j <- 0 until values1.size){
          values1(j) should be(values2(j))
        }
      }
    }

  }

  it should " provide a proper sliding window operator." in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 2000L
    val deltaTMillis  = 200L
    val nPartitions   = 8

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaTMillis,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val flattenedRawTS = rawTS
      .collect()
      .foldRight(List[(TSInstant, DenseVector[Double])]())(_ :: _)
      .toArray

    val sliceFromRawTS = flattenedRawTS.sliding(5, 5).toArray

    val cutPredicate = (x: TSInstant, y: TSInstant) => x.timestamp.secondOfDay != y.timestamp.secondOfDay

    val slicedRDD = overlappingRDD
      .mapValues(v => v.slicingWindow(Array(cutPredicate))(x => x).toArray)
      .map(_._2)
      .map(_.map(_._2))
      .collect()
      .map(_.toList)
      .foldRight(List[Array[(TSInstant, DenseVector[Double])]]())(_ ::: _)
      .toArray

    sliceFromRawTS.length should be (slicedRDD.length + 1) // We count intervals

    for((data1, data2) <- sliceFromRawTS.zip(slicedRDD)){
      data1.length should be (data2.length)
      for(((ts1, values1), (ts2, values2)) <- data1.zip(data2)) {
        ts1.timestamp.getMillis should be(ts2.timestamp.getMillis)
        for(j <- 0 until values1.size){
          values1(j) should be(values2(j))
        }
      }
    }

  }

  it should " properly slide fold elements with memory" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val zeroArray   = Array.fill(nColumns)(0.0)
    val zeroSecond  = TSInstant(new DateTime(0L))

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    def sumArray(data: Array[(TSInstant, DenseVector[Double])], state: Double): (Double, Double) = {
      (data.map(x => sum(x._2)).sum * state, state)
    }

    val foldFromOverlappingData = overlappingRDD
      .mapValues(v => v.slidingFoldWithMemory(Array(IntervalSize(1.0, 1.0)))(sumArray, 0.0, (x: Double, y: Double) => x + y, 1.5))
      .map(_._2)
      .reduce(_ + _)

    foldFromOverlappingData should be ((3 * (nSamples - 2) + 2 * 2) * nColumns * 1.5)

  }

  it should " properly slide fold elements with memory and exteral targets" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val zeroArray   = Array.fill(nColumns)(0.0)
    val zeroSecond  = TSInstant(new DateTime(0L))

    val rawTS = Surrogate.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    val (overlappingRDD, _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    def sumArray(data: Array[(TSInstant, DenseVector[Double])], state: Double): (Double, Double) = {
      (data.map(x => sum(x._2)).sum * state, state)
    }

    val foldFromOverlappingData = overlappingRDD
      .mapValues(v => v.slidingFoldWithMemory(Array(IntervalSize(1.0, 1.0)), v.locations.slice(v.firstValidIndex, v.lastValidIndex + 1))(sumArray, 0.0, (x: Double, y: Double) => x + y, 1.5))
      .map(_._2)
      .reduce(_ + _)

    foldFromOverlappingData should be ((3 * (nSamples - 2) + 2 * 2) * nColumns * 1.5)

  }

}