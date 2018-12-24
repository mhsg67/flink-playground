package com.paytm.daas.datastream.playground

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Simple Join learning
  */
object Driver1 {

  def splitTwoPart(in: String): (Long, Long) = {
    val parts = in.split(",")
    (parts(0).trim.toLong, parts(1).trim.toLong)
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val userStream = env.readTextFile("/Users/mohammad/Projects/flink-playground/src/main/resources/USER")
      .map(x => splitTwoPart(x))

    val navStream = env.readTextFile("/Users/mohammad/Projects/flink-playground/src/main/resources/NAV")
      .map(x => splitTwoPart(x))

    val userStreamWithTime = userStream.assignTimestampsAndWatermarks(new MyTimestampAssigner)
    val navStreamWithTime = navStream.assignTimestampsAndWatermarks(new MyTimestampAssigner)

    val joinedStream = navStreamWithTime.join(userStreamWithTime)
      .where(navElm => navElm._1)
      .equalTo(userElm => userElm._2)
      .window(SlidingEventTimeWindows.of(Time.milliseconds(2000), Time.milliseconds(1000)))
      .apply { (nav, users) => (nav._1, users._1, nav._2) }

    joinedStream.print()
    env.execute()

  }
}

class MyTimestampAssigner extends AssignerWithPunctuatedWatermarks[(Long, Long)] {

  override def extractTimestamp(event: (Long, Long), previousElementTimestamp: Long): Long = {
    return System.currentTimeMillis()
  }

  override def checkAndGetNextWatermark(lastElement: (Long, Long), extractedTimestamp: Long): Watermark = {
    new Watermark(System.currentTimeMillis() - 3000)
  }
}