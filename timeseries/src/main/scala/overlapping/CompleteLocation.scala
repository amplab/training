package main.scala.overlapping

/**
 * Fundamental data structure when doing main.scala.overlapping result reductions
 * in a sane manner (redundant results are not computed).
 * An admissible point about which compute the value of a valid kernel
 * will havve partIdx == originIdx.
 */
case class CompleteLocation[KeyT](partIdx: Int, originIdx: Int, k: KeyT)
