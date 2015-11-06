package main.scala.overlapping.timeSeries

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures.{InnovationAlgo, Toeplitz}

import scala.reflect.ClassTag


/**
 * Will return AR coeffs followed by MA coeffs.
 */
object ARMAModel{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      p: Int,
      q: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): Array[SecondOrderSignature] = {

    implicit val sc = timeSeries.context
    val estimator = new ARMAModel[IndexT](p, q, mean)
    estimator.estimate(timeSeries)

  }

}

class ARMAModel[IndexT <: Ordered[IndexT] : ClassTag](
    p: Int,
    q: Int,
    mean: Option[DenseVector[Double]] = None)
   (implicit config: TSConfig, sc: SparkContext)
  extends AutoCovariances[IndexT](p + q, mean){

  /*
  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 243)
   */
  def getMACoefs(psiCoeffs: DenseVector[Double], aCoeffs: DenseVector[Double]): DenseVector[Double] ={

    val MACoefs = DenseVector.zeros[Double](q)

    for(j <- 0 until q){
      MACoefs(j) = psiCoeffs(j)
      for(i <- 1 until (j min p)){
        MACoefs(j) -= aCoeffs(i - 1) * psiCoeffs(j - i)
      }
      if(p >= j){
        MACoefs(j) -= aCoeffs(j)
      }
    }

    MACoefs
  }

  /*
  TODO: there is an issue here whenever most pre-estimation thetas are zero. Need to use another estimation procedure.
   */
  def computeARMACoeffs(autoCovs: SecondOrderSignature): SecondOrderSignature = {

    val signaturePQ = InnovationAlgo(p + q, autoCovs.covariation)

    val coeffsAR: DenseVector[Double] = Toeplitz(
      p,
      signaturePQ.covariation(q - p to q + p - 2),
      signaturePQ.covariation(q to q + p - 1))

    val coeffsMA: DenseVector[Double] = getMACoefs(signaturePQ.covariation, coeffsAR)

    val coeffs: DenseVector[Double] = DenseVector.vertcat(coeffsAR, coeffsMA)

    SecondOrderSignature(coeffs, signaturePQ.variation)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[SecondOrderSignature]= {

    super
      .estimate(timeSeries)
      .map(computeARMACoeffs)

  }

}