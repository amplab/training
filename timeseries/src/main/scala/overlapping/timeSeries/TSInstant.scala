package main.scala.overlapping.timeSeries

import org.joda.time.DateTime

/**
 * Ordered timestamps.
 */
case class TSInstant(timestamp: DateTime) extends Ordered[TSInstant]{

  override def compare(that: TSInstant): Int = {
    this.timestamp.compareTo(that.timestamp)
  }

}
