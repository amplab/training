/*

package main.scala.overlapping

import scala.reflect.ClassTag

/**
 * Implementation of the replicator in the case of an ordered index as in time series.
 */
class SingleAxisLateralReplicator[IndexT <: Ordered[IndexT], ValueT: ClassTag]
(
  override val intervals: Array[(IndexT, IndexT)],
  override val signedDistance: (IndexT, IndexT) => Double,
  override val padding: (Double, Double),
  lateralPartitions: Array[Array[Int]]
  )
  extends SingleAxisReplicator[IndexT, Array[ValueT]](intervals, signedDistance, padding){

  val nLateralPartitions = lateralPartitions.length

  /*
  main.scala.overlapping block indexing scheme:
  n_block * block_index + time_interval_index
   */

  def mainAxisReplication(k: IndexT, lateralPartitionIdx: Int): List[(Int, Int, IndexT)] = {
    val intervalLocation = getIntervalLocation(k)

    val centralIdx = intervalLocation.intervalIdx * nLateralPartitions + lateralPartitionIdx
    var result = (centralIdx, lateralPartitionIdx, k) :: Nil

    if((intervalLocation.offset <= padding._1) &&
      (intervalLocation.ahead >= 0.0) &&
      (intervalLocation.intervalIdx > 0)){
      val backwardIdx = (intervalLocation.intervalIdx - 1) * nLateralPartitions + lateralPartitionIdx
      result = (backwardIdx, centralIdx, k) :: result
    }

    if((intervalLocation.ahead <= padding._2) &&
      (intervalLocation.offset >= 0.0) &&
      (intervalLocation.intervalIdx < intervals.length - 1)){
      val forwardIdx = (intervalLocation.intervalIdx + 1) * nLateralPartitions + lateralPartitionIdx
      result = (forwardIdx, lateralPartitionIdx, k) :: result
    }

    result
  }

  def buildDatum(selectedData: Array[ValueT], replicationIdx: (Int, Int, IndexT)): ((Int, Int, IndexT), Array[ValueT]) = {
    (replicationIdx, selectedData)
  }

  override def replicate(k: IndexT, v: Array[ValueT]): Iterator[((Int, Int, IndexT), Array[ValueT])] = {
    var result = List[((Int, Int, IndexT), Array[ValueT])]()

    for((selectedIdxs, lateralPartitionIdx) <- lateralPartitions.zipWithIndex){
      val selectedData        = selectedIdxs.map(i => v(i))
      val replicationIndices  = mainAxisReplication(k, lateralPartitionIdx)

      result = replicationIndices.map(x => buildDatum(selectedData, x)) ::: result
    }

    result.toIterator

  }


}

*/