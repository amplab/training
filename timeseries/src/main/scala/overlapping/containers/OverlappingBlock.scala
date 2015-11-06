package main.scala.overlapping.containers

import main.scala.overlapping.{CompleteLocation, IntervalSize}

import scala.reflect.ClassTag

/**
 *  Main trait of the main.scala.overlapping weak memory data project.
 *  The distributed representation of data will be based on a key, value RDD of
 *  (int, OverlappingBlock) where the key is the index of the partition.
 */
trait OverlappingBlock[KeyT, ValueT] extends Serializable{

  /**
   * Raw data.
   *
   * @return Raw representation of data (with padding).
   */
  def data: Array[(KeyT, ValueT)]

  /**
   * Locations that keep track of the current partition index and the original partition index.
   * Redundant elements will have different partition index and original partition index.
   *
   * @return Partition aware representation of keys.
   */
  def locations: Array[CompleteLocation[KeyT]] // Can be evenly spaced or not

  /**
   * Array of distance functions (one per layout axis).
   *
   * @return The distance functions.
   */
  def signedDistances: Array[((KeyT, KeyT) => Double)]

  /**
   * Apply the kernel f with a window width of size (array of sizes, one per layout axis),
   * centering the kernel computation on the targets.
   *
   * @param size Width of kernel window.
   * @param targets Targets the kernel computations will be centered about.
   * @param f The kernel function that will be applied to each kernel window.
   * @tparam ResultT Kernel return type
   * @return A time series with the resulting kernel computations. (Padding not guaranteed).
   */
  def sliding[ResultT: ClassTag](
      size: Array[IntervalSize],
      targets: Array[CompleteLocation[KeyT]])
      (f: Array[(KeyT, ValueT)] => ResultT): OverlappingBlock[KeyT, ResultT]

  /*
  Roll apply the kernel f centering all computations on the admissible data points
  of the main.scala.overlapping block.
   */
  def sliding[ResultT: ClassTag](size: Array[IntervalSize])
      (f: Array[(KeyT, ValueT)] => ResultT): OverlappingBlock[KeyT, ResultT]

  /*
  Here there is no roll apply. An array of predicates decides on when to separate a new window
  in the data and compute the kernel f on it.
   */
  def slicingWindow[ResultT: ClassTag](
      cutPredicates: Array[(KeyT, KeyT) => Boolean])
      (f: Array[(KeyT, ValueT)] => ResultT): OverlappingBlock[KeyT, ResultT]

  def filter(p: (KeyT, ValueT) => Boolean): OverlappingBlock[KeyT, ValueT]

  def map[ResultT: ClassTag](f: (KeyT, ValueT) => ResultT): OverlappingBlock[KeyT, ResultT]

  def reduce(f: ((KeyT, ValueT), (KeyT, ValueT)) => (KeyT, ValueT)): (KeyT, ValueT)

  def fold[ResultT: ClassTag](zeroValue: ResultT)(f: (KeyT, ValueT) => ResultT,
                                                  op: (ResultT, ResultT) => ResultT): ResultT

  /*
  Directly fold the results of a kernel operation in order to reduce memory burden.
   */
  def slidingFold[ResultT: ClassTag](
      size: Array[IntervalSize],
      targets: Array[CompleteLocation[KeyT]])
      (f: Array[(KeyT, ValueT)] => ResultT,
       zero: ResultT,
       op: (ResultT, ResultT) => ResultT): ResultT


  /*
  Same thing, the targets being all the admissible points of the block.
   */
  def slidingFold[ResultT: ClassTag](size: Array[IntervalSize])
      (f: Array[(KeyT, ValueT)] => ResultT,
       zero: ResultT,
       op: (ResultT, ResultT) => ResultT): ResultT

  /*
  Compute kernel f on all the windows devised by the array of predicates and fold the results.
   */
  def slicingWindowFold[ResultT: ClassTag](cutPredicates: Array[(KeyT, KeyT) => Boolean])
      (f: Array[(KeyT, ValueT)] => ResultT,
       zero: ResultT,
       op: (ResultT, ResultT) => ResultT): ResultT

  def toArray: Array[(KeyT, ValueT)] // Guarantees there is no redundancy here

  def count: Long

  def take(n: Int): Array[(KeyT, ValueT)]

}
