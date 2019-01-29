name                                    := "flink-playground"
version                                 := "0.1-SNAPSHOT"
organization                            := "ca.mhsg"
resolvers                               += "Confluent Repository" at "http://packages.confluent.io/maven/"
assembly / mainClass                    := Some("ca.mhsg.playground.Driver3")
Compile / run                           := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
Compile / run / fork                    := false
Global / cancelable                     := true
assembly / assemblyOption               := (assembly / assemblyOption).value.copy(includeScala = false)
parallelExecution       in    Test      := false
parallelExecution       in    Test      := false

ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)
ThisBuild / scalaVersion := "2.11.12"


val flinkVersion          = "1.7.0"
val jacksonVersion        = "2.6.0"

val avro                    = "org.apache.avro"                 %  "avro"                               % "1.8.2"
val flinkKafkaConnector     = "org.apache.flink"                %% "flink-connector-kafka"              % flinkVersion
val flinkCassandraConnector = "org.apache.flink"                %% "flink-connector-cassandra"          % flinkVersion
val flinkRocksDb            = "org.apache.flink"                %% "flink-statebackend-rocksdb"         % flinkVersion
val flinkScala              = "org.apache.flink"                %% "flink-scala"                        % flinkVersion    
val flinkStreamingScala     = "org.apache.flink"                %% "flink-streaming-scala"              % flinkVersion    
val flinkTestUtil           = "org.apache.flink"                %% "flink-test-utils"                   % flinkVersion    % Test
val flinkToSchemaRegistry   = "org.apache.flink"                %  "flink-avro-confluent-registry"      % flinkVersion    exclude("com.fasterxml.jackson.core", "jackson-databind")
val jacksonDatabind         = "com.fasterxml.jackson.core"      %  "jackson-databind"                   % jacksonVersion
val jacksonJoda             = "com.fasterxml.jackson.datatype"  %  "jackson-datatype-joda"              % jacksonVersion
val jacksonScala            = "com.fasterxml.jackson.module"    %% "jackson-module-scala"               % jacksonVersion
val kafkaAvroSeri           = "io.confluent"                    %  "kafka-avro-serializer"              % "4.1.1"         exclude("com.fasterxml.jackson.core", "jackson-databind")
val scalaTest               = "org.scalatest"                   %% "scalatest"                          % "3.0.1"         % Test
val stringTemplate          = "org.antlr"                       %  "stringtemplate"                     % "3.2"


lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= Seq(avro, flinkKafkaConnector, flinkRocksDb, flinkScala, flinkStreamingScala,
      flinkTestUtil, flinkToSchemaRegistry, jacksonDatabind, jacksonJoda, jacksonScala, kafkaAvroSeri, scalaTest,
      stringTemplate, flinkCassandraConnector),
      assemblyMergeStrategy in assembly := {
      case PathList("about.html") =>
        MergeStrategy.rename
      case x if x.endsWith("io.netty.versions.properties") => 
        MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )