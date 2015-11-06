package main.scala.overlapping.spatial


import breeze.linalg.{sum, DenseMatrix}

import scala.reflect.ClassTag

/**
 *
 * @param minLon
 * @param minLat
 * @param maxLon
 * @param maxLat
 * @param nSampleMatrix Same orientation as the map (rows go from South to North, cols go from East to West)
 * @param minNSamples
 * @tparam T
 */
class QuadTree[T: ClassTag](
    minLon: Double,
    minLat: Double,
    maxLon: Double,
    maxLat: Double,
    nSampleMatrix: DenseMatrix[Double],
    minNSamples: Long) extends Serializable{

  private var NE: Option[QuadTree[T]] = None
  private var SE: Option[QuadTree[T]] = None
  private var SW: Option[QuadTree[T]] = None
  private var NW: Option[QuadTree[T]] = None

  val nSamples = sum(nSampleMatrix)

  val splitLon = 0.5 * (minLon + maxLon)
  val splitLat = 0.5 * (minLat + maxLat)

  if(nSamples > minNSamples){
    val nCols = nSampleMatrix.cols
    val nRows = nSampleMatrix.rows

    val splitCol = nCols / 2
    val splitRow = nRows / 2

    NE = Some(new QuadTree[T](
      splitLon, splitLat, maxLon, maxLat,
      nSampleMatrix(splitRow until nRows, splitCol until nCols),
      minNSamples))

    SE = Some(new QuadTree[T](
      minLon, splitLat, splitLon, maxLat,
      nSampleMatrix(0 until splitRow, splitCol until nCols),
      minNSamples))

    SW = Some(new QuadTree[T](
      minLon, minLat, splitLon, splitLat,
      nSampleMatrix(0 until splitRow, 0 until splitCol),
      minNSamples))

    NW = Some(new QuadTree[T](
      splitLon, minLat, maxLon, splitLat,
      nSampleMatrix(splitRow until nRows, 0 until splitCol),
      minNSamples))

  }

  def align(lon: Double, lat: Double): (Double, Double) = {

    if(NE.isEmpty){
      return (splitLon, splitLat)
    }

    if(lon >= splitLon && lat >= splitLat){
      return NE.get.align(lon, lat)
    }

    if(lon >= splitLon && lat <= splitLat){
      return SE.get.align(lon, lat)
    }

    if(lon <= splitLon && lat <= splitLat){
      return SW.get.align(lon, lat)
    }

    return NW.get.align(lon, lat)

  }

  def getLeafLocations(): Array[(Double, Double)] = {

    if(NE.isEmpty){
      return Array((splitLon, splitLat))
    }

    NE.get.getLeafLocations() ++ SE.get.getLeafLocations() ++ SW.get.getLeafLocations() ++ NW.get.getLeafLocations()

  }


}
