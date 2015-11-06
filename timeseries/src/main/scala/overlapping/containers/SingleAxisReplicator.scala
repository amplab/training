package main.scala.overlapping.containers

import main.scala.overlapping.Replicator

import scala.reflect.ClassTag

/**
 * Implementation of the replicator in the case of an ordered index as in time series.
 */
class
SingleAxisReplicator[IndexT <: Ordered[IndexT], ValueT: ClassTag]
  (
    val intervals: Array[(IndexT, IndexT)],
    val signedDistance: (IndexT, IndexT) => Double,
    val padding: (Double, Double)
  )
  extends Replicator[IndexT, ValueT]{

  case class IntervalLocation(intervalIdx: Int, offset: Double, ahead: Double)

  def getIntervalIdx(i: IndexT): Int ={

    val firstIdx = intervals.apply(0)._1
    if (i.compareTo(firstIdx) < 0) {
      return 0
    }

    for(((intervalStart, intervalEnd), intervalIdx)  <- intervals.zipWithIndex) {
      if ((i.compareTo(intervalStart) >= 0) && (i.compareTo(intervalEnd) <= 0)) {
        return intervalIdx
      }
    }

    intervals.length - 1
  }

  def getIntervalLocation(i: IndexT): IntervalLocation ={

    val (firstIdx, _) = intervals.head
    if (i.compareTo(firstIdx) < 0) {
      return IntervalLocation(
        0,
        signedDistance(firstIdx, i), // This offset will be negative
        signedDistance(i, firstIdx))
    }

    for(((intervalStart, intervalEnd), intervalIdx)  <- intervals.zipWithIndex) {
      if ((i.compareTo(intervalStart) >= 0) && (i.compareTo(intervalEnd) <= 0)) {
        return IntervalLocation(
          intervalIdx,
          signedDistance(intervalStart, i),
          signedDistance(i, intervalEnd))
      }
    }

    val (_, lastTimestamp) = intervals.last
    IntervalLocation(
      intervals.length - 1,
      signedDistance(lastTimestamp, i),
      signedDistance(i, lastTimestamp)) // This look ahead will be negative

  }

  override def replicate(k: IndexT, v: ValueT): Iterator[((Int, Int, IndexT), ValueT)] = {

    val intervalLocation = getIntervalLocation(k)

    var result = ((intervalLocation.intervalIdx, intervalLocation.intervalIdx, k), v) :: Nil

    if((intervalLocation.offset <= padding._1) &&
      (intervalLocation.ahead >= 0.0) &&
      (intervalLocation.intervalIdx > 0)){
      result = ((intervalLocation.intervalIdx - 1, intervalLocation.intervalIdx, k), v) :: result
    }

    if((intervalLocation.ahead <= padding._2) &&
      (intervalLocation.offset >= 0.0) &&
      (intervalLocation.intervalIdx < intervals.size - 1)){
      result = ((intervalLocation.intervalIdx + 1, intervalLocation.intervalIdx, k), v) :: result
    }

    result.toIterator
  }


}
