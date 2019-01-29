package ca.mhsg.playground

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.{Collector, TernaryBoolean}

/**
  * RocksDB backend experiment - Just want to see how much space it takes
  * Run it in flink cluster
  * First time:
  *   ./flink run flink-playground-assembly-0.1-SNAPSHOT.jar
  * Second time:
  *   ./flink run -s checkpoint_path flink-playground-assembly-0.1-SNAPSHOT.jar
  */
object Driver3 {

  def toTransactionEvent(in: String): OwnershipEvent = {
    val parts = in.split(",")
    OwnershipEvent(parts(0).trim.toLong, Owning(parts(1).trim.toLong, parts(2).trim.toLong))
  }

  def main(args: Array[String]): Unit = {
    val checkpointDir = "file:///Users/mohammad/Documents/remove/checkpoint"
    val stateBackend = new RocksDBStateBackend(new FsStateBackend(checkpointDir, true), TernaryBoolean.TRUE)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(20 * 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointTimeout(24 * 60 * 60 * 1000)

    env.setStateBackend(stateBackend)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 100))
    env.setParallelism(1)

    val transactionStream = env.socketTextStream("localhost", 9999)
      .map(x => toTransactionEvent(x))

    val userStreamWithTime = transactionStream.
      assignTimestampsAndWatermarks(new GenericStreamTimestampAssigner[OwnershipEvent]).keyBy(_.navId)

    val sinkId = "sync_" + System.currentTimeMillis()
    userStreamWithTime.flatMap(new NavToUserStateRDB).print(sinkId)
    env.execute()

  }
}

class NavToUserStateRDB extends RichFlatMapFunction[OwnershipEvent, PortfolioUpdateEvent] {

  private lazy val navToUserState: MapState[Long, Long] =
    getRuntimeContext.getMapState(new MapStateDescriptor[Long, Long]("User owns unit of nav2", createTypeInformation[Long], createTypeInformation[Long]))

  override def flatMap(value: OwnershipEvent, out: Collector[PortfolioUpdateEvent]): Unit = {
    if(value.owning.userId == 3)
      throw new Exception("None sense!")
    val currentHolding = navToUserState.get(value.owning.userId)
    val newHolding = currentHolding + value.owning.share
    navToUserState.put(value.owning.userId, newHolding)
    out.collect(PortfolioUpdateEvent(value.owning.userId, value.navId, newHolding, 1))
  }
}


