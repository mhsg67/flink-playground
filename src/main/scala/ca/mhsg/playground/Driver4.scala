package ca.mhsg.playground

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  * Experiment with timer-service
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


    val debitTransactionsStream = env.socketTextStream("localhost", 9999)
      .map(x => toDebitTrans(x))
      .assignTimestampsAndWatermarks(new TransactionStreamTimestampAssigner[DebitTrans])
      .keyBy(_.transId)
    val creditTransactionsStream = env.socketTextStream("localhost", 9998)
      .map(x => toCreditTrans(x))
      .assignTimestampsAndWatermarks(new TransactionStreamTimestampAssigner[CreditTrans])
      .keyBy(_.transId)

    val connectedStream: ConnectedStreams[DebitTrans, CreditTrans] = debitTransactionsStream.connect(creditTransactionsStream)
  }
}

