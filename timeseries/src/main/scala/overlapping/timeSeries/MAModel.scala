package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures.InnovationAlgo

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */

object MAModel{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      q: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): Array[SecondOrderSignature] ={

    implicit val sc = timeSeries.context
    val estimator = new MAModel[IndexT](q, mean)
    estimator.estimate(timeSeries)

  }

}

class MAModel[IndexT <: Ordered[IndexT] : ClassTag](
    q: Int,
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends AutoCovariances[IndexT](q, mean){

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[SecondOrderSignature]= {

    super
      .estimate(timeSeries)
      .map(x => InnovationAlgo(q, x.covariation))

  }


}