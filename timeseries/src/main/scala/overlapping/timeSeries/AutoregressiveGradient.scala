package main.scala.overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.overlapping._

/**
 * Created by Francois Belletti on 9/24/15.
 */
class AutoregressiveGradient[IndexT <: Ordered[IndexT]](
    p: Int,
    gradientFunction: (Array[DenseMatrix[Double]], Array[(IndexT, DenseVector[Double])]) => Array[DenseMatrix[Double]],
    dim: Option[Int] = None)
    (implicit config: TSConfig)
extends SecondOrderEssStat[IndexT, DenseVector[Double], Array[DenseMatrix[Double]]]
{

  val d = dim.getOrElse(config.d)
  val x = Array.fill(p){DenseMatrix.zeros[Double](d, d)}

  val gradientSizes = x.map(y => (y.rows, y.cols))

  def kernelWidth = IntervalSize(p * config.deltaT, 0)

  def modelOrder = ModelSize(p, 0)

  def zero = gradientSizes.map({case (nRows, nCols) => DenseMatrix.zeros[Double](nRows, nCols)})

  def setNewX(newX: Array[DenseMatrix[Double]]) = {
    val maxEigenValue = Stability(newX)

    for(i <- x.indices){
      x(i) := newX(i) / maxEigenValue
    }
  }

  def getGradientSize = gradientSizes

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): Array[DenseMatrix[Double]] = {

    if(slice.length != modelWidth){
      return gradientSizes.map({case (r, c) => DenseMatrix.zeros[Double](r, c)})
    }
    gradientFunction(x, slice)

  }

  override def reducer(x: Array[DenseMatrix[Double]], y: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={
    x.zip(y).map({case (x, y) => x + y})
  }

}
