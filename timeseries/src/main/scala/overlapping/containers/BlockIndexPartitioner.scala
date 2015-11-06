package main.scala.overlapping.containers

import org.apache.spark.Partitioner

/**
 * This computes partition hash value based on complete locations simply
 * by retrieving the first element of the complete location.
 *
 * @param numPartitions The number of partitions desired by the user.
 *
 */
class BlockIndexPartitioner(override val numPartitions: Int)
  extends Partitioner{

  def getPartition(key: Any): Int = key match {
    case key: (Int, _, _) => key._1
    case _ => throw new UnsupportedOperationException("Trying to block partition with invalid key")
  }

}
