/*

package main.scala.overlapping.timeSeries.pointProcesses


import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/3/15.
 */

class HayashiYoshida[LeftRecordType: ClassTag, RightRecordType: ClassTag](
                 leftTS: TimeSeries[LeftRecordType],
                 rightTS: TimeSeries[RightRecordType]) extends Serializable{


  def computeCrossFoldHY[ResultType: ClassTag](crossCov: ((LeftRecordType, LeftRecordType),
                                                          (RightRecordType, RightRecordType)) => ResultType,
                                               leftVol: ((LeftRecordType, LeftRecordType)) => ResultType,
                                               rightVol: ((RightRecordType, RightRecordType)) => ResultType,
                                               foldOp: (ResultType, ResultType) => ResultType,
                                               zero: ResultType)
                                              (cLeft: Int, cRight: Int, lagMillis: Long): (ResultType, ResultType, ResultType) ={

    if(lagMillis < 0){
      return computeCrossFoldHY(crossCov, leftVol, rightVol, foldOp, zero)(cRight, cLeft, -lagMillis)
    }

    val leftTimestamps  = leftTS.timestampTiles
    val leftValues      = leftTS.dataTiles

    val rightTimestamps = rightTS.timestampTiles
    val rightValues     = rightTS.dataTiles

    def computeCrossFoldPartition(partitionLeftTimestamps: Iterator[TSInstant],
                                  partitionRightTimestamps: Iterator[TSInstant],
                                  partitionLeftValues: Iterator[Array[LeftRecordType]],
                                  partitionRightValues: Iterator[Array[RightRecordType]]): Iterator[(ResultType, ResultType, ResultType)] = {

      if(rightTS.config.memory.value < lagMillis){
        throw new IndexOutOfBoundsException("Time series' memory is below lag")
      }

      val (partitionLeftTimestampsIt, partitionLeftTimestampsTemp) = partitionLeftTimestamps.duplicate
      val firstLeftTS                 = partitionLeftTimestampsTemp.next()
      val leftPartitionLastTimestamp  = leftTS.partitioner.getLastTimestampOfPartition(firstLeftTS).getMillis

      val (partitionRightTimestampsIt, partitionRightTimestampsTemp) = partitionRightTimestamps.duplicate
      val firstRightTS                = partitionRightTimestampsTemp.next()
      val rightPartitionLastTimestamp = rightTS.partitioner.getLastTimestampOfPartition(firstRightTS).getMillis

      val leftIntervals: Iterator[(Long, Long)] = partitionLeftTimestampsIt
        .sliding(2, 1)
        .map(x => (x.head.getMillis + lagMillis, x.last.getMillis + lagMillis))
      val rightIntervals: Iterator[(Long, Long)] = partitionRightTimestampsIt
        .sliding(2, 1)
        .map(x => (x.head.getMillis, x.last.getMillis))

      val leftDeltas: Iterator[(LeftRecordType, LeftRecordType)] = partitionLeftValues
        .drop(cLeft)
        .next
        .sliding(2, 1)
        .map(x => (x.last, x.head))
      val rightDeltas: Iterator[(RightRecordType, RightRecordType)] = partitionRightValues
        .drop(cRight)
        .next
        .sliding(2, 1)
        .map(x => (x.last, x.head))

      var covariation = zero
      var leftVariation = zero
      var rightVariation = zero

      if (leftIntervals.hasNext && rightIntervals.hasNext) {

        var stopLeft  = false
        var stopRight = false

        var leftInterval = leftIntervals.next()
        var leftDelta = leftDeltas.next()
        leftVariation = foldOp(leftVariation, leftVol(leftDelta))

        var rightInterval = rightIntervals.next()
        var rightDelta = rightDeltas.next()
        rightVariation = foldOp(rightVariation, rightVol(rightDelta))

        while((!stopLeft) &&
          (leftInterval._1 <= leftPartitionLastTimestamp)){

          stopRight = false
          // Right interval is not completely after left
          while ((rightInterval._1 < leftInterval._2) &&
            (!stopRight) &&
            (rightInterval._1 <= leftPartitionLastTimestamp + lagMillis)) {

            if (rightInterval._2 > leftInterval._1) {
              // Right interval is not completely before left
              covariation = foldOp(covariation, crossCov(leftDelta, rightDelta))
            }

            if (rightIntervals.hasNext) {
              rightInterval = rightIntervals.next()
              rightDelta = rightDeltas.next()
              if(rightInterval._1 <= leftPartitionLastTimestamp + lagMillis)
                rightVariation = foldOp(rightVariation, rightVol(rightDelta))
            } else {
              stopRight = true
            }
          }

          if (!leftIntervals.hasNext) {
            stopLeft = true
          } else {
            leftInterval = leftIntervals.next()
            leftDelta = leftDeltas.next()
            if(leftInterval._1 <= leftPartitionLastTimestamp)
              leftVariation = foldOp(leftVariation, leftVol(leftDelta))
          }
        }

      }

      ((leftVariation, rightVariation, covariation) :: Nil).toIterator
    }

    leftTimestamps.zipPartitions(rightTimestamps, leftValues, rightValues, true)(computeCrossFoldPartition)
      .fold((zero, zero, zero))({case ((lv1, rv1, cv1), (lv2, rv2, cv2)) => (foldOp(lv1, lv2), foldOp(rv1, rv2), foldOp(cv1, cv2))})

  }

}

*/