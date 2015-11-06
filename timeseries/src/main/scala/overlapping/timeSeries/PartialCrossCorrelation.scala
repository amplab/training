package main.scala.overlapping.timeSeries

import breeze.linalg._
import breeze.numerics.sqrt
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.procedures.ToeplitzMulti

import scala.reflect.ClassTag


object PartialCrossCorrelation{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      maxLag: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    implicit val sc = timeSeries.context
    val estimator = new PartialCrossCorrelation[IndexT](maxLag, mean)
    estimator.estimate(timeSeries)

  }

}


/**
Here we expect the number of dimensions to be the same for all records.

The autocovoriance is ordered as follows

-modelOrder ... 0 ... modelOrder
 */

class PartialCrossCorrelation[IndexT <: Ordered[IndexT] : ClassTag](
    maxLag: Int,
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends CrossCovariance[IndexT](maxLag, mean){

  def estimatePrecisionMatrices(crossCovMatrices: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={
    val nCols = crossCovMatrices.head.rows

    val coeffMatrices = ToeplitzMulti(maxLag, nCols,
      crossCovMatrices.slice(1, 2 * maxLag),
      crossCovMatrices.slice(maxLag+ 1, 2 * maxLag + 1))

    coeffMatrices.foreach(x => x := x.t)

    coeffMatrices
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    (Array[DenseMatrix[Double]], DenseMatrix[Double])={

    val covarianceMatrices = normalize(
      timeSeriesStats(timeSeries)
    )

    val partialAutoCovs = estimatePrecisionMatrices(covarianceMatrices)

    (partialAutoCovs, partialAutoCovs(0))

  }


}