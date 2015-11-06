/*

package main.scala.overlapping.timeSeries.secondOrder.largeSystem

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import main.scala.overlapping.containers.block.SingleAxisBlock
import main.scala.overlapping.timeSeries.secondOrder.SecondOrderEssStat
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.VARGradientDescent

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/16/15.
 */
class LateralSplitVARGradientDescent[IndexT <: Ordered[IndexT] : ClassTag](
  val modelOrder: Int,
  val deltaT: Double,
  val lateralPartitions: Array[Array[Int]],
  val partitionStride: Int,
  val lossFunctions: Array[(Array[DenseMatrix[Double]], Array[(IndexT, DenseVector[Double])]) => Double],
  val gradientFunctions: Array[(Array[DenseMatrix[Double]], Array[(IndexT, DenseVector[Double])]) => Array[DenseMatrix[Double]]],
  val gradientSizeArray: Array[Array[(Int, Int)]],
  val stepSize: Int => Double,
  val precision: Double,
  val maxIter: Int,
  val start: Array[Array[DenseMatrix[Double]]])
  extends SecondOrderEssStat[IndexT, DenseVector[Double]]{

    val gradientDescents = lossFunctions
      .indices
      .map(i => new VARGradientDescent(
      modelOrder,
      deltaT,
      lossFunctions(i),
      gradientFunctions(i),
      gradientSizeArray(i),
      stepSize,
      precision,
      maxIter,
      start(i)
    ))

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[Array[DenseMatrix[Double]]] = {

    timeSeries.persist(MEMORY_AND_DISK)

    val parameters = lateralPartitions.indices.map(
      x => gradientDescents(x).estimate(
        timeSeries.filter(_._1 % partitionStride == 0)
      )
    )

    timeSeries.unpersist(false)

    parameters.toArray

  }

}

*/
