package main.scala.overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.procedures.InnovationAlgoMulti

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */

object VMAModel{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      q: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    implicit val sc = timeSeries.context
    val estimator = new VMAModel[IndexT](q, mean)
    estimator.estimate(timeSeries)

  }

}

class VMAModel[IndexT <: Ordered[IndexT] : ClassTag](
    q: Int,
    mean: Option[DenseVector[Double]] = None)
  (implicit config: TSConfig, sc: SparkContext)
  extends CrossCovariance[IndexT](q, mean){

  def estimateVMAMatrices(crossCovMatrices: Array[DenseMatrix[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    InnovationAlgoMulti(q, crossCovMatrices)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(timeSeries)._1
    estimateVMAMatrices(crossCovMatrices)

  }

}