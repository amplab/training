package main.scala.overlapping.containers

import main.scala.overlapping.{CompleteLocation, IntervalSize}

import scala.reflect.ClassTag
import scala.util.Random


object SingleAxisBlock{

  /*
  The data will be sorted with respect to keys.
   */
  def apply[IndexT <: Ordered[IndexT], ValueT: ClassTag]
    (rawData: Array[((Int, Int, IndexT), ValueT)],
    signedDistance: (IndexT, IndexT) => Double): SingleAxisBlock[IndexT, ValueT] ={

    val sortedData: Array[((Int, Int, IndexT), ValueT)] = rawData
      .sortBy(_._1._3)

    val data: Array[(IndexT, ValueT)] = sortedData
      .map({case (k, v) => (k._3, v)})

    val locations: Array[CompleteLocation[IndexT]] = sortedData
      .map({case (k, v) => CompleteLocation(k._1, k._2, k._3)})

    val signedDistances: Array[((IndexT, IndexT) => Double)] = Array(signedDistance)

    new SingleAxisBlock[IndexT, ValueT](data, locations, signedDistances)

  }

}


class SingleAxisBlock[IndexT <: Ordered[IndexT], ValueT: ClassTag](
    val data: Array[(IndexT, ValueT)],
    val locations: Array[CompleteLocation[IndexT]],
    val signedDistances: Array[(IndexT, IndexT) => Double])
  extends OverlappingBlock[IndexT, ValueT]{

  lazy val firstValidIndex = locations.indexWhere(x => x.partIdx == x.originIdx)

  lazy val lastValidIndex  = locations.lastIndexWhere(x => x.partIdx == x.originIdx, locations.length - 1)

  val signedDistance = signedDistances.apply(0)


  override def sliding[ResultT: ClassTag](size: Array[IntervalSize])
                               (f: Array[(IndexT, ValueT)] => ResultT): SingleAxisBlock[IndexT, ResultT] = {

    sliding[ResultT](size, locations.slice(firstValidIndex, lastValidIndex + 1))(f)

  }

  def getWindowIndex(beginIndex: Int, endIndex: Int, t: IndexT, lookBack: Double, lookAhead: Double): (Int, Int) ={

    var begin_index = beginIndex
    var end_index = endIndex

    begin_index = locations.indexWhere(x => signedDistance(x.k, t) <= lookBack,
      begin_index)

    end_index = locations.indexWhere(x => signedDistance(t, x.k) > lookAhead,
      end_index)

    if (end_index == -1) {
      end_index = data.length - 1
    }

    end_index = locations.lastIndexWhere(x => signedDistance(t, x.k) <= lookAhead,
      end_index)

    (begin_index, end_index)

  }


  override def sliding[ResultT: ClassTag](
      size: Array[IntervalSize],
      targets: Array[CompleteLocation[IndexT]])
      (f: Array[(IndexT, ValueT)] => ResultT): SingleAxisBlock[IndexT, ResultT] = {

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = 0

    val result = Array.ofDim[(IndexT, ResultT)](targets.length)
    var valid_results = Array.fill(targets.length)(false).zipWithIndex

    for((center_location , i) <- targets.zipWithIndex){

      if(end_index != 1) {

        val (begin_index_, end_index_) = getWindowIndex(begin_index, end_index, center_location.k, lookBack, lookAhead)
        begin_index = begin_index_
        end_index = end_index_

        if ((begin_index != -1) && (end_index != -1)) {
          result(i) = (center_location.k, f(data.slice(begin_index, end_index + 1)))
          valid_results(i) = (true, i)
        }

      }

    }

    valid_results = valid_results.filter(x => x._1)

    new SingleAxisBlock[IndexT, ResultT](
      valid_results.map(i => result(i._2)),
      valid_results.map(i => targets(i._2)),
      signedDistances)

  }

  def slidingWithMemory[ResultT: ClassTag, MemType: ClassTag](
        size: Array[IntervalSize],
        targets: Array[CompleteLocation[IndexT]])
       (f: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
        init: MemType): SingleAxisBlock[IndexT, ResultT] = {

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = 0

    val result = Array.ofDim[(IndexT, ResultT)](targets.length)
    var valid_results = Array.fill(targets.length)(false).zipWithIndex
    var mem = init

    var i = 0
    while(i < targets.length){

      val center_location = targets(i)
      i += 1

      if(end_index != 1) {

        val (begin_index_, end_index_) = getWindowIndex(begin_index, end_index, center_location.k, lookBack, lookAhead)
        begin_index = begin_index_
        end_index = end_index_

        if ((begin_index != -1) && (end_index != -1)) {
          val (temp_res, temp_mem) = f(data.slice(begin_index, end_index + 1), mem)
          mem = temp_mem
          result(i) = (center_location.k, temp_res)

          valid_results(i) = (true, i)
        }

      }

    }

    valid_results = valid_results.filter(x => x._1)

    new SingleAxisBlock[IndexT, ResultT](
      valid_results.map(i => result(i._2)),
      valid_results.map(i => targets(i._2)),
      signedDistances)

  }

  def slidingWithMemory[ResultT: ClassTag, MemType: ClassTag](
        size: Array[IntervalSize])
       (f: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
        init: MemType): SingleAxisBlock[IndexT, ResultT] = {

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = 0

    val valid_locations = locations.slice(firstValidIndex, lastValidIndex + 1)
    val result = Array.ofDim[(IndexT, ResultT)](valid_locations.length)
    var mem = init

    var data_idx = 0
    var result_idx = 0

    while(data_idx <= lastValidIndex){

      val center_location = locations(data_idx)

      if(end_index != -1){

        val (begin_index_, end_index_) = getWindowIndex(begin_index, end_index, center_location.k, lookBack, lookAhead)
        begin_index = begin_index_
        end_index = end_index_

        if ((begin_index != -1) && (end_index != -1)) {
          val (temp_res, temp_mem) = f(data.slice(begin_index, end_index + 1), mem)
          mem = temp_mem

          if(data_idx >= firstValidIndex) {
            result(result_idx) = (center_location.k, temp_res)
            result_idx += 1
          }

        }

      }

      data_idx += 1

    }

    new SingleAxisBlock[IndexT, ResultT](
      result,
      valid_locations.slice(0, result_idx),
      signedDistances.slice(0, result_idx))

  }

  def slidingFold[ResultT: ClassTag](size: Array[IntervalSize],
                                     targets: Array[CompleteLocation[IndexT]])
                                    (f: Array[(IndexT, ValueT)] => ResultT,
                                     zero: ResultT,
                                     op: (ResultT, ResultT) => ResultT): ResultT = {

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = 0

    var result = zero

    var i = 0
    while(i < targets.length){
      val center_location = targets(i)
      i += 1

      if(end_index != -1) {

        val (begin_index_, end_index_) = getWindowIndex(begin_index, end_index, center_location.k, lookBack, lookAhead)
        begin_index = begin_index_
        end_index = end_index_

        if ((begin_index != -1) && (end_index != -1)) {
          result = op(result, f(data.slice(begin_index, end_index + 1)))
        }

      }

    }

    result
  }

  def slidingFold[ResultT: ClassTag](size: Array[IntervalSize])
                                    (f: Array[(IndexT, ValueT)] => ResultT,
                                     zero: ResultT,
                                     op: (ResultT, ResultT) => ResultT): ResultT = {

    slidingFold(size, locations.slice(firstValidIndex, lastValidIndex + 1))(f, zero, op)

  }


  // By convention the marking CompleteLocation of a slice will be that of the start of the interval
  override def slicingWindow[ResultT: ClassTag](
      cutPredicates: Array[(IndexT, IndexT) => Boolean])
      (f: Array[(IndexT, ValueT)] => ResultT): SingleAxisBlock[IndexT, ResultT] ={

    val cutPredicate = cutPredicates.head

    var begin_index = 0
    var end_index   = 0

    var resultLocations = List[CompleteLocation[IndexT]]()
    var resultData      = List[(IndexT, ResultT)]()

    val intervals = locations.zip(locations.drop(1))

    while((end_index < intervals.length) && (end_index != -1)){
      end_index = intervals.indexWhere({case (x, y) => cutPredicate(x.k, y.k)}, begin_index)

      if(end_index != -1) {

        val start = locations(begin_index)
        val stop = locations(end_index)

        resultLocations = resultLocations :+ CompleteLocation[IndexT](start.partIdx, start.originIdx, start.k)

        resultData = resultData :+ (start.k, f(data.slice(begin_index, end_index + 1)))

        begin_index = end_index + 1
      }
    }

    new SingleAxisBlock[IndexT, ResultT](resultData.toArray, resultLocations.toArray, signedDistances)

  }

  def slidingFoldWithMemory[ResultT: ClassTag, MemType: ClassTag](
      size: Array[IntervalSize],
      targets: Array[CompleteLocation[IndexT]])
     (f: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      init: MemType): ResultT = {

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = 0

    var result = zero
    var mem = init

    for((center_location , i) <- targets.zipWithIndex){

      if(end_index != -1) {

        val (begin_index_, end_index_) = getWindowIndex(begin_index, end_index, center_location.k, lookBack, lookAhead)
        begin_index = begin_index_
        end_index = end_index_

        if ((begin_index != -1) && (end_index != -1)) {
          val (temp_res, temp_mem) = f(data.slice(begin_index, end_index + 1), mem)
          mem = temp_mem
          result = op(result, temp_res)
        }

      }
    }

    result

  }

  def slidingFoldWithMemory[ResultT: ClassTag, MemType: ClassTag](
      size: Array[IntervalSize])
     (f: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      init: MemType): ResultT = {

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = 0

    var result = zero
    var mem = init

    var data_idx = 0

    while(data_idx <= lastValidIndex){

      val center_location = locations(data_idx)

      if(end_index != -1) {

        val (begin_index_, end_index_) = getWindowIndex(begin_index, end_index, center_location.k, lookBack, lookAhead)
        begin_index = begin_index_
        end_index = end_index_

        if ((begin_index != -1) && (end_index != -1)) {
          val (temp_res, temp_mem) = f(data.slice(begin_index, end_index + 1), mem)
          mem = temp_mem

          if(data_idx >= firstValidIndex) {
            result = op(result, temp_res)
          }

        }

      }

      data_idx += 1

    }

    result

  }

  def slicingWindowFold[ResultT: ClassTag](cutPredicates: Array[(IndexT, IndexT) => Boolean])(
      f: Array[(IndexT, ValueT)] => ResultT,
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT): ResultT = {

    val cutPredicate = cutPredicates.head

    var begin_index = 0
    var end_index   = 0
    var result      = zero

    val intervals = locations.zip(locations.drop(1))

    while((end_index < intervals.length) && (end_index != -1)){
      end_index = intervals.indexWhere({case (x, y) => cutPredicate(x.k, y.k)}, begin_index)

      if(end_index != -1) {

        val start = locations(begin_index)
        val stop = locations(end_index)

        result = op(result, f(data.slice(begin_index, end_index + 1)))

        begin_index = end_index + 1
      }
    }

    result

  }


  override def filter(p: (IndexT, ValueT) => Boolean): SingleAxisBlock[IndexT, ValueT] = {

    val p_ = p.tupled

    val validIndices = data.indices.filter(i => p_(data(i))).toArray

    new SingleAxisBlock[IndexT, ValueT](
      validIndices.map(i => data(i)),
      validIndices.map(i => locations(i)),
      signedDistances)

  }

  /**
   *
   * @param f
   * @tparam ResultT
   * @return
   */
  override def map[ResultT: ClassTag](f: (IndexT, ValueT) => ResultT): SingleAxisBlock[IndexT, ResultT] = {

    val result = new SingleAxisBlock[IndexT, ResultT](data.map({case (k, v) => (k, f(k, v))}), locations, signedDistances)

    result

  }

  override def reduce(f: ((IndexT, ValueT), (IndexT, ValueT)) => (IndexT, ValueT)): (IndexT, ValueT) = {

    data.slice(firstValidIndex, lastValidIndex + 1).reduce(f)

  }

  override def fold[ResultT: ClassTag](zeroValue: ResultT)(f: (IndexT, ValueT) => ResultT,
                                                           op: (ResultT, ResultT) => ResultT): ResultT = {

    var result = zeroValue
    for(i <- firstValidIndex to lastValidIndex){
      result = op(result, f(data(i)._1, data(i)._2))
    }

    result

  }

  override def toArray: Array[(IndexT, ValueT)] = {

    data.slice(firstValidIndex, lastValidIndex + 1)

  }

  override def count: Long = {

    lastValidIndex + 1 - firstValidIndex

  }

  override def take(n : Int): Array[(IndexT, ValueT)] = {

    data.slice(firstValidIndex, lastValidIndex + 1).take(n)

  }


  /*
def sparseSliding[ResultT: ClassTag](size: Array[IntervalSize],
                                     targets: Array[CompleteLocation[Int]])
                                     (f: Array[(IndexT, ValueT)] => ResultT): SingleAxisBlock[IndexT, ResultT] = {

  val lookAhead = size.head.lookAhead
  val lookBack  = size.head.lookBack

  var begin_index = 0
  var end_index   = 0

  var result = List[(IndexT, ResultT)]()

  for(center_location <- targets) {

    begin_index = locations.lastIndexWhere(x => signedDistance(x.k, data(center_location.k)._1) < lookBack,
      center_location.k) + 1
    end_index = locations.indexWhere(x => signedDistance(data(center_location.k)._1, x.k) >= lookAhead,
      center_location.k)

    if ((begin_index != -1) && (end_index != -1)) {
      result = result :+(data(center_location.k)._1, f(data.slice(begin_index, end_index + 1)))
    }
  }

  new SingleAxisBlock[IndexT, ResultT](result.toArray, targets.map(x => locations(x.k)), signedDistances)

}

def sparseSlidingFold[ResultT: ClassTag](size: Array[IntervalSize],
                                         targets: Array[CompleteLocation[Int]])
                                         (f: Array[(IndexT, ValueT)] => ResultT,
                                          zero: ResultT,
                                          op: (ResultT, ResultT) => ResultT): ResultT = {

  val lookAhead = size.head.lookAhead
  val lookBack  = size.head.lookBack

  var begin_index = -1
  var end_index   = -1

  var result = zero

  for(center_location <- targets) {

    begin_index = locations.lastIndexWhere(x => signedDistance(x.k, data(center_location.k)._1) < lookBack,
      center_location.k) + 1
    end_index = locations.indexWhere(x => signedDistance(data(center_location.k)._1, x.k) >= lookAhead,
      center_location.k)

    if ((begin_index != -1) && (end_index != -1)) {
      result = op(result, f(data.slice(begin_index, end_index + 1)))
    }
  }

  result
}

def randSlidingFold[ResultT: ClassTag](size: Array[IntervalSize])
                                      (f: Array[(IndexT, ValueT)] => ResultT,
                                       zero: ResultT,
                                       op: (ResultT, ResultT) => ResultT,
                                       batchSize: Int): ResultT = {

  if(batchSize >= lastValidIndex - firstValidIndex){
    slidingFold(size, locations.slice(firstValidIndex, lastValidIndex + 1))(f, zero, op)
  }else{
    val nValid = lastValidIndex - firstValidIndex + 1
    val selectedLocations = Array.fill(batchSize){Random.nextInt(nValid)}
      .map(i => {
      val targetCompleteLocation = locations(firstValidIndex + i)
      CompleteLocation(targetCompleteLocation.partIdx, targetCompleteLocation.originIdx, i)
    })
    sparseSlidingFold(size, selectedLocations)(f, zero, op)
  }

}
*/

}
