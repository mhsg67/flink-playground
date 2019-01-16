package ca.mhsg.playground

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * Simple state management learning
  */
object Driver2 {

  def toTransactionEvent(in: String): OwnershipEvent = {
    val parts = in.split(",")
    OwnershipEvent(parts(0).trim.toLong, Owning(parts(1).trim.toLong, parts(2).trim.toLong))
  }

  def toNavUpdateEvent(in: String): NavUpdateEvent = {
    val parts = in.split(",")
    NavUpdateEvent(parts(0).trim.toLong, parts(1).trim.toLong)
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)

    val transactionStream = env.socketTextStream("localhost", 9999)
      .map(x => toTransactionEvent(x))

    val navStream = env.socketTextStream("localhost", 9998)
      .map(x => toNavUpdateEvent(x))

    val userStreamWithTime = transactionStream.assignTimestampsAndWatermarks(new GenericStreamTimestampAssigner[OwnershipEvent]).keyBy(_.navId)
    val navStreamWithTime = navStream.assignTimestampsAndWatermarks(new GenericStreamTimestampAssigner[NavUpdateEvent]).keyBy(_.navId)
    val connectedStream: ConnectedStreams[OwnershipEvent, NavUpdateEvent] = userStreamWithTime.connect(navStreamWithTime)

    connectedStream.flatMap(new NavToUserState2()).print()

    env.execute()

  }
}


class NavToUserState2 extends RichCoFlatMapFunction[OwnershipEvent, NavUpdateEvent, PortfolioUpdateEvent] {
  private lazy val navToUserState: MapState[Long, Long] =
    getRuntimeContext.getMapState(new MapStateDescriptor[Long, Long]("User owns unit of nav2", createTypeInformation[Long], createTypeInformation[Long]))

  override def flatMap1(value: OwnershipEvent, out: Collector[PortfolioUpdateEvent]): Unit = {
    val currentHolding = navToUserState.get(value.owning.userId)
    navToUserState.put(value.owning.userId, currentHolding + value.owning.share)
  }

  override def flatMap2(value: NavUpdateEvent, out: Collector[PortfolioUpdateEvent]): Unit = {
    val owningList2 = navToUserState.iterator().asScala.toList
    owningList2.foreach(o =>
      out.collect(
        PortfolioUpdateEvent(value.navId, o.getKey, value.unitValue, value.unitValue * o.getValue)
      )
    )
  }
}

class NavToUserState extends RichCoFlatMapFunction[OwnershipEvent, NavUpdateEvent, PortfolioUpdateEvent] {

  private lazy val navToUserState: ListState[Owning] =
    getRuntimeContext.getListState(new ListStateDescriptor[Owning]("User owns unit of nav", createTypeInformation[Owning]))

  override def flatMap1(value: OwnershipEvent, out: Collector[PortfolioUpdateEvent]): Unit =
    navToUserState.add(value.owning.copy())

  override def flatMap2(value: NavUpdateEvent, out: Collector[PortfolioUpdateEvent]): Unit = {
    val owningList: List[Owning] = navToUserState.get().asScala.toList
    owningList.foreach(o =>
      out.collect(
        PortfolioUpdateEvent(value.navId, o.share, value.unitValue, value.unitValue * o.share)
      )
    )
  }
}

