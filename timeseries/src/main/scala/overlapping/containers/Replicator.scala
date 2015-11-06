package main.scala.overlapping

/**
  *  A replicator creates main.scala.overlapping data in main.scala.overlapping blocks
  *  with respect to a certain partitioning scheme.
  */
trait Replicator[KeyT, ValueT] extends Serializable{

  /**
   * This can duplicate a datum if need be.
   * @param k Original key (generally unique but not necessary)
   * @param v Original value
   * @return A sequence of ((origin partition, current partition, k), v)
   */
  def replicate(k: KeyT, v: ValueT): TraversableOnce[((Int, Int, KeyT), ValueT)]

}
