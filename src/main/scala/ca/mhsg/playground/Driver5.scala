package ca.mhsg.playground

import java.util.Properties

import ca.mhsg.playground.Constant._
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import scala.collection.JavaConverters._

/**
  * Reading Generic Avro record from Kafka with confluent schema registry
  *
  STEPS:

  sudo vim /etc/hosts
    127.0.0.1   kafka
    127.0.0.1   schema-registry

  docker exec -it schema-registry /bin/bash
    kafka-avro-console-producer --broker-list kafka:9092 --topic source-topic --property value.schema='{"type":"record","name":"testRecord","fields":[{"name":"id","type":"long"}]}'

  docker exec -it schema-registry /bin/bash
     kafka-avro-console-consumer --topic sink-topic --bootstrap-server kafka:9092 --from-beginning
  */
object Driver5 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val kafkaStream = env.addSource(buildKafkaSource(SourceTopic))
    kafkaStream.filter(x => x.get("id").asInstanceOf[Long] % 2 == 0).addSink(buildKafkaSink(SinkTopic))

    env.execute()
  }

  def buildKafkaSource(topic: String): FlinkKafkaConsumer[GenericRecord] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", KafkaBootstrapServers)
    val deserializationSchema = ConfluentRegistryAvroDeserializationSchema.forGeneric(ConfluentSchemaManager.getValueSchema(topic), SchemaRegistryUrl)
    val source = new FlinkKafkaConsumer[GenericRecord](topic, deserializationSchema, properties)
    source.setStartFromEarliest()
    source
  }

  def buildKafkaSink(topic: String): FlinkKafkaProducer[GenericRecord] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", KafkaBootstrapServers)
    val serializationSchema = new ConfluentRegistryAvroSerializationSchema(SchemaRegistryUrl, topic)
    new FlinkKafkaProducer[GenericRecord](topic, serializationSchema, properties)
  }
}

class ConfluentRegistryAvroSerializationSchema(schemaRegistryUrl: String,
                                                  topic: String) extends SerializationSchema[GenericRecord] {
  @transient lazy val kafkaAvroSerializer: KafkaAvroSerializer = {
    val schemaClient = new CustomCachedSchemaRegistryClient(SchemaRegistryUrl, 8)
    val map = Map("schema.registry.url" -> schemaRegistryUrl, "auto.register.schemas" -> "true", "max.schemas.per.subject" -> "8")
    val serializer = new KafkaAvroSerializer(schemaClient)
    serializer.configure(map.asJava, false)
    serializer
  }

  override def serialize(element: GenericRecord): Array[Byte] = kafkaAvroSerializer.serialize(topic, element)
}