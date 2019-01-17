package ca.mhsg.playground

import java.util.Properties

import ca.mhsg.playground.Constant._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, MapperOptions}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * Sink to Casssandra
  *
STEPS:
  docker exec -it cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

  docker exec -it cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS test_keyspace.test_table_tuple (id bigint, name text, age int, title text, PRIMARY KEY (id, name));"

  docker exec -it cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS test_keyspace.test_table_case_class (id bigint, name text, age int, title text, PRIMARY KEY (id, name));"

  docker exec -it cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS test_keyspace.test_table_pojo (id bigint, name text, age int, title text, PRIMARY KEY (id, name));"

  docker exec -it schema-registry /bin/bash
    kafka-avro-console-producer --broker-list kafka:9092 \
    --topic source-topic --property value.schema='{
        "type":"record","name":"testRecord","fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"},
        {"name":"age","type":"int"},
        {"name":"title","type":"string"}
      ]
    }'

  sudo vim /etc/hosts
    127.0.0.1   cassandra
  */
object Driver6 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val kafkaStream = env.addSource(buildKafkaSource(SourceTopic))

    val tupledStream: DataStream[(String, Long, Int, String)] =
      kafkaStream
        .map(x => (x.get("name").toString, x.get("id").asInstanceOf[Long], x.get("age").asInstanceOf[Int], x.get("title").toString))

    CassandraSink
      .addSink(tupledStream)
      .setQuery("INSERT INTO test_keyspace.test_table_tuple(name, id, age, title) values (?, ?, ?, ?);")
      .setHost("cassandra", 9042)
      .build()


    val caseClassStream: DataStream[Employee] =
      kafkaStream
        .map(x => Employee(x.get("id").asInstanceOf[Long], x.get("name").toString, x.get("title").toString, x.get("age").asInstanceOf[Int]))

    //NOTE: Order of column names in query should match order of them in case class definition
    CassandraSink
      .addSink(caseClassStream)
      .setHost("cassandra", 9042)
      .setQuery("INSERT INTO test_keyspace.test_table_case_class(id, name, title, age) values (?, ?, ?, ?);")
      .build()

    /*
        import com.datastax.driver.core.ConsistencyLevel
        import com.datastax.driver.mapping.Mapper
        import com.datastax.driver.mapping.annotations.{Column, Table}

        val pojoStream: DataStream[JEmployee] =
          kafkaStream
            .map(x => new JEmployee(x.get("id").asInstanceOf[Long], x.get("name").toString, x.get("title").toString, x.get("age").asInstanceOf[Int]))

        val cassandraOptions = Array[Mapper.Option](
          Mapper.Option.saveNullFields(false),
          Mapper.Option.consistencyLevel(ConsistencyLevel.ONE)
        )

        CassandraSink
          .addSink(pojoStream)
          .setMapperOptions(new MapperOptions {
            override def getMapperOptions: Array[Mapper.Option] = cassandraOptions
          })
          .setHost("cassandra", 9042)
          .build()
    */

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
}

case class Employee(eId: Long, eName: String, eTitle: String, eAge: Int)

