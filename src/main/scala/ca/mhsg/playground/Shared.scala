package ca.mhsg.playground

import ca.mhsg.playground.Constant.SchemaRegistryUrl
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

trait HasEventTime {
  def getEventTime: Long
}
case class DebitTrans(transId: Long, timestamp: Long, accountId: String, amount: String) extends HasEventTime {
  override def getEventTime: Long = timestamp
}
case class CreditTrans(transId: Long, timestamp: Long, creditCardId: String, holderName: String, amount: String) extends HasEventTime {
  override def getEventTime: Long = timestamp
}

class TransactionStreamTimestampAssigner[A<:HasEventTime] extends AssignerWithPunctuatedWatermarks[A] {
  override def extractTimestamp(event: A, previousElementTimestamp: Long): Long = event.getEventTime
  override def checkAndGetNextWatermark(lastElement: A, extractedTimestamp: Long): Watermark = new Watermark(lastElement.getEventTime - 3000)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

object Constant {
  val SchemaRegistryUrl = "http://schema-registry:8082"
  val SourceTopic = "source-topic"
  val SinkTopic = "sink-topic"
  val KafkaBootstrapServers = "kafka:9092"
}

object ConfluentSchemaManager {
  private val schemaClient = new CachedSchemaRegistryClient(SchemaRegistryUrl, 8)

  def getValueSchema(topic: String): Schema = getSchema(topic + "-value")

  def getSchema(subject: String): Schema = schemaClient.getById(schemaClient.getLatestSchemaMetadata(subject).getId)
}