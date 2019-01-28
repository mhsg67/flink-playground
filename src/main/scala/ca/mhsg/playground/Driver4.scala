package ca.mhsg.playground

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * Experiment with timer-service
  * I've tried to mimic session timeout in this scenario, where each transaction is composed of a
  * debit payment and credit payment. If we receive the debit/credit payment but during the next 10 seconds
  * there is no matching credit/debit payment for the received one, this will publish event with transaction id
  */
object Driver4 {

  def toDebitTrans(in: String): DebitTrans = {
    val parts = in.split(",")
    DebitTrans(parts(0).trim.toLong, parts(1).trim.toLong, parts(2).trim, parts(3).trim)
  }

  def toCreditTrans(in: String): CreditTrans = {
    val parts = in.split(",")
    CreditTrans(parts(0).trim.toLong, parts(1).trim.toLong, parts(2).trim, parts(3).trim, parts(4).trim)
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)
    val checkDelayedArrivalUdf = new checkDelayArrivalUdf(10000)

    val debitTransactionsStream = env.socketTextStream("localhost", 9999)
      .map(x => toDebitTrans(x))
      .assignTimestampsAndWatermarks(new TransactionStreamTimestampAssigner[DebitTrans])
      .keyBy(_.transId)
    val creditTransactionsStream = env.socketTextStream("localhost", 9998)
      .map(x => toCreditTrans(x))
      .assignTimestampsAndWatermarks(new TransactionStreamTimestampAssigner[CreditTrans])
      .keyBy(_.transId)

    val connectedStream = debitTransactionsStream.connect(creditTransactionsStream)
      .map(x => GenericTransaction(x.transId, x.timestamp, x.amount), y => GenericTransaction(y.transId, y.timestamp, y.amount))
      .keyBy(_.transId)

    connectedStream.process(checkDelayedArrivalUdf).print()
    env.execute()
  }
}

class checkDelayArrivalUdf(ttl: Long) extends KeyedProcessFunction[Long, GenericTransaction, GenericTransaction] {

  private lazy val state: MapState[Long, GenericTransaction] =
    getRuntimeContext.getMapState(new MapStateDescriptor[Long, GenericTransaction]("Transaction that has arrived", classOf[Long], classOf[GenericTransaction]))

  override def processElement(value: GenericTransaction, ctx: KeyedProcessFunction[Long, GenericTransaction, GenericTransaction]#Context, out: Collector[GenericTransaction]): Unit = {
    if(!state.contains(value.transId)){
      state.put(value.transId, value)
      registerTimer(ctx)
    }
    else {
      state.remove(value.transId)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, GenericTransaction, GenericTransaction]#OnTimerContext, out: Collector[GenericTransaction]): Unit = {
    val key = ctx.getCurrentKey
    if (state.contains(key))
      out.collect(state.get(key))
  }

  private def registerTimer(ctx: KeyedProcessFunction[Long, GenericTransaction, GenericTransaction]#Context): Unit = {
    ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + ttl)
  }
}