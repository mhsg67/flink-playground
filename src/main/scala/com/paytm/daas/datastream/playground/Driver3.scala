package com.paytm.daas.datastream.playground

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

/**
  * RocksDB backend experiment - Just want to see how much space it takes
  */
object Driver3 {

  def toTransactionEvent(in: String): OwnershipEvent = {
    val parts = in.split(",")
    OwnershipEvent(parts(0).trim.toLong, Owning(parts(1).trim.toLong, parts(2).trim.toLong))
  }

  def main(args: Array[String]): Unit = {
    val checkpointDir = "file:///Users/mohammad/Projects/flink-playground/src/main/resources/db"
    val stateBackend = new RocksDBStateBackend(checkpointDir, true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(stateBackend)
    env.setParallelism(1)

    val transactionStream = env.socketTextStream("localhost", 9999)
      .map(x => toTransactionEvent(x))

    val userStreamWithTime = transactionStream.
      assignTimestampsAndWatermarks(new GenericStreamTimestampAssigner[OwnershipEvent]).keyBy(_.navId)

    userStreamWithTime.flatMap(new NavToUserStateRDB).print()
    env.execute()

  }
}

class NavToUserStateRDB extends RichFlatMapFunction[OwnershipEvent, PortfolioUpdateEvent] {

  private lazy val navToUserState: MapState[Long, Long] =
    getRuntimeContext.getMapState(new MapStateDescriptor[Long, Long]("User owns unit of nav2", createTypeInformation[Long], createTypeInformation[Long]))

  override def flatMap(value: OwnershipEvent, out: Collector[PortfolioUpdateEvent]): Unit = {
    val currentHolding = navToUserState.get(value.owning.userId)
    navToUserState.put(value.owning.userId, currentHolding + value.owning.share)
  }
}


