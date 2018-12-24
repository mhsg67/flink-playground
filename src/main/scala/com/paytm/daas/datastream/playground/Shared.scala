package com.paytm.daas.datastream.playground

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

case class Owning(userId: Long, share: Long)
case class NavUpdateEvent(navId: Long, unitValue: Long)
case class OwnershipEvent(navId: Long, owning: Owning)
case class PortfolioUpdateEvent(userId: Long, navId: Long, unitValue: Long, newValuation: Long)


class GenericStreamTimestampAssigner[T] extends AssignerWithPunctuatedWatermarks[T] {
  override def extractTimestamp(event: T, previousElementTimestamp: Long): Long = System.currentTimeMillis()
  override def checkAndGetNextWatermark(lastElement: T, extractedTimestamp: Long): Watermark = new Watermark(System.currentTimeMillis() - 3000)
}