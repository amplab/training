package main.scala.overlapping.timeSeries

import breeze.linalg._
import breeze.stats.distributions.Rand
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * Simulate data based on different models.
 */
object Surrogate {


  def generateWhiteNoise(nColumns: Int,
                         nSamples: Int,
                         deltaTMillis: Long,
                         noiseGen: Rand[Double],
                         magnitudes: DenseVector[Double],
                         sc: SparkContext):
  RDD[(TSInstant, DenseVector[Double])] = {
    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)),
                 magnitudes :* DenseVector(noiseGen.sample(nColumns).toArray)))
    sc.parallelize(rawData)
  }

  def generateOnes(nColumns: Int,
                   nSamples: Int,
                   deltaTMillis: Long,
                   sc: SparkContext):
  RDD[(TSInstant, DenseVector[Double])] = {
    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)),
                 DenseVector.ones[Double](nColumns)))
    sc.parallelize(rawData)
  }

  def generateAR(phis: Array[DenseVector[Double]],
                 nColumns:Int,
                 nSamples: Int,
                 deltaTMillis: Long,
                 noiseGen: Rand[Double],
                 magnitudes: DenseVector[Double],
                 sc: SparkContext): RDD[(TSInstant, DenseVector[Double])] = {

    val d = phis.length
    val p = phis(0).length

    val phiMatrices = Array.fill(p)(DenseMatrix.zeros[Double](d, d))

    for(i <- 0 until p){
      for(j <- 0 until d){
        phiMatrices(i)(j, j) = phis(j)(i)
      }
    }

    generateVAR(phiMatrices, nColumns, nSamples, deltaTMillis, noiseGen, magnitudes, sc)

  }

  def generateMA(thetas: Array[DenseVector[Double]],
                 nColumns: Int,
                 nSamples: Int,
                 deltaTMillis: Long,
                 noiseGen: Rand[Double],
                 magnitudes: DenseVector[Double],
                 sc: SparkContext): RDD[(TSInstant, DenseVector[Double])] = {

    val d = thetas.length
    val p = thetas(0).length

    val thetaMatrices = Array.fill(p)(DenseMatrix.zeros[Double](d, d))

    for(i <- 0 until p){
      for(j <- 0 until d){
        thetaMatrices(i)(j, j) = thetas(j)(i)
      }
    }

    generateVMA(thetaMatrices, nColumns, nSamples, deltaTMillis, noiseGen, magnitudes, sc)

  }

  def generateARMA(phis: Array[DenseVector[Double]],
                   thetas: Array[DenseVector[Double]],
                   nColumns: Int,
                   nSamples: Int,
                   deltaTMillis: Long,
                   noiseGen: Rand[Double],
                   magnitudes: DenseVector[Double],
                   sc: SparkContext): RDD[(TSInstant, DenseVector[Double])] = {


    val d = thetas.length
    val p = thetas(0).length

    val phiMatrices = Array.fill(p)(DenseMatrix.zeros[Double](d, d))

    for(i <- 0 until p){
      for(j <- 0 until d){
        phiMatrices(i)(j, j) = phis(j)(i)
      }
    }

    val thetaMatrices = Array.fill(p)(DenseMatrix.zeros[Double](d, d))

    for(i <- 0 until p){
      for(j <- 0 until d){
        thetaMatrices(i)(j, j) = thetas(j)(i)
      }
    }

    generateVARMA(phiMatrices, thetaMatrices, nColumns, nSamples, deltaTMillis, noiseGen, magnitudes, sc)

  }

  def generateVAR(phis: Array[DenseMatrix[Double]],
                  nColumns:Int,
                  nSamples: Int,
                  deltaTMillis: Long,
                  noiseGen: Rand[Double],
                  magnitudes: DenseVector[Double],
                  sc: SparkContext):
  RDD[(TSInstant, DenseVector[Double])] = {

    val p = phis.length

    val noiseMatrix = new DenseMatrix(nColumns, nSamples, noiseGen.sample(nSamples * nColumns).toArray)

    for(i <- p until nSamples){
      for(h <- 1 to p){
        noiseMatrix(::, i) += phis(h - 1) * (magnitudes :* noiseMatrix(::, i - h))
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(::, x).copy))

    sc.parallelize(rawData)
  }

  def generateVMA(thetas: Array[DenseMatrix[Double]],
                  nColumns: Int,
                  nSamples: Int,
                  deltaTMillis: Long,
                  noiseGen: Rand[Double],
                  magnitudes: DenseVector[Double],
                  sc: SparkContext):
  RDD[(TSInstant, DenseVector[Double])] = {

    val q = thetas.length

    val noiseMatrix = new DenseMatrix(nColumns, nSamples, noiseGen.sample(nSamples * nColumns).toArray)
    for(i <- (nSamples - 1) to 1 by -1){
      for(h <- 1 to q) {
        noiseMatrix(::, i) :+= thetas(h - 1) * (magnitudes :* noiseMatrix(::, i - h))
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(::, x).copy))

    sc.parallelize(rawData)

  }

  def generateVARMA(phis: Array[DenseMatrix[Double]],
                    thetas: Array[DenseMatrix[Double]],
                    nColumns: Int,
                    nSamples: Int,
                    deltaTMillis: Long,
                    noiseGen: Rand[Double],
                    magnitudes: DenseVector[Double],
                    sc: SparkContext): RDD[(TSInstant, DenseVector[Double])] = {

    val q = thetas.length
    val p = phis.length

    val noiseMatrix = new DenseMatrix(nColumns, nSamples, noiseGen.sample(nSamples * nColumns).toArray)
    for(i <- (nSamples - 1) to q by -1){
      for(h <- 1 to q) {
        noiseMatrix(::, i) :+= thetas(h - 1) * (magnitudes :* noiseMatrix(::, i - h))
      }
    }

    for(i <- p until nSamples){
      for(h <- 1 to p){
        noiseMatrix(::, i) :+= phis(h - 1) * noiseMatrix(::, i - h)
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(::, x).copy))

    sc.parallelize(rawData)

  }

}
