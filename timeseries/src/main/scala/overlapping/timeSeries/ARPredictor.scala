package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers.SingleAxisBlock

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
object ARPredictor{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      matrices: Array[DenseVector[Double]],
      mean: Option[DenseVector[Double]])
      (implicit config: TSConfig): RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])] = {

    implicit val sc = timeSeries.context
    val predictor = new ARPredictor[IndexT](matrices, mean)
    predictor.estimateResiduals(timeSeries)

  }

}

class ARPredictor[IndexT <: Ordered[IndexT] : ClassTag](
    matrices: Array[DenseVector[Double]],
    mean: Option[DenseVector[Double]])
    (implicit config: TSConfig, sc: SparkContext)
  extends Predictor[IndexT]{

  val p = matrices(0).length
  val deltaT = config.deltaT
  val d = matrices.length
  if(d != config.d){
    throw new IndexOutOfBoundsException("AR matrix dimensions and time series dimension not compatible.")
  }
  val bcMatrices = sc.broadcast(matrices)
  val bcMean = sc.broadcast(mean.getOrElse(DenseVector.zeros[Double](d)))

  def size: Array[IntervalSize] = Array(IntervalSize(p * deltaT, 0))

  override def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {

    val pred = bcMean.value.copy

    for (i <- 0 until (data.length - 1)) {
      for (j <- 0 until d) {
        pred(j) += bcMatrices.value(j)(data.length - 2 - i) * (data(i)._2(j) - bcMean.value(j))
      }
    }

    pred

  }

}