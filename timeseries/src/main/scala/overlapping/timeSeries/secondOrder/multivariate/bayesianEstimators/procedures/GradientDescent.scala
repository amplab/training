package main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.procedures

import breeze.linalg._
import breeze.numerics.sqrt
import main.scala.overlapping.timeSeries.Stability

/**
 * Created by Francois Belletti on 7/14/15.
 */

object GradientDescent extends Serializable{

  def run[DataT](lossFunction: (Array[DenseMatrix[Double]], DataT) => Double,
                 gradientFunction: (Array[DenseMatrix[Double]], DataT) => Array[DenseMatrix[Double]],
                 gradientSizes: Array[(Int, Int)],
                 stepSize: Int => Double,
                 precision: Double,
                 maxIter: Int,
                 start: Array[DenseMatrix[Double]],
                 data: DataT,
                 maxIterLs: Int = 10): Array[DenseMatrix[Double]] ={


    val alpha = 0.4
    val beta = 0.9

    var prevLoss = lossFunction(start, data)
    var nextLoss = prevLoss

    var firstIter = true

    var parameters = start
    var gradient = Array.fill(gradientSizes.length) {
      DenseMatrix.zeros[Double](0, 0)
    }

    var step = gradient.clone()
    var t = 0.0

    var gradientMagnitude = 0.0
    var i = 0
    var iLs = 0

    while (firstIter || ((i <= maxIter) && (sqrt(gradientMagnitude) > precision * (1 + nextLoss)))) {

      prevLoss = nextLoss
      gradient = gradientFunction(parameters, data)

      gradientMagnitude = gradient.map({case x: DenseMatrix[Double] => sum(x :* x)}).sum

      t = stepSize(i)
      step = gradient.map(x => x * t)
      nextLoss = lossFunction(parameters.indices.toArray.map(x => parameters(x) - step(x)), data)

      iLs = 0
      while((nextLoss > prevLoss + alpha * t * t * gradientMagnitude) && (iLs < maxIterLs)){
        t = beta * t
        step = gradient.map(x => x * t)
        nextLoss = lossFunction(parameters.indices.toArray.map(x => parameters(x) - step(x)), data)

        iLs += 1
      }

      parameters = parameters.indices.toArray.map(x => parameters(x) - step(x))

      i = i + 1

      firstIter = false
    }

    parameters
  }

}
