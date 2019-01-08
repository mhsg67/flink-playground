FROM openjdk:8
RUN wget http://apache.mirror.globo.tech/flink/flink-1.7.1/flink-1.7.1-bin-hadoop27-scala_2.11.tgz
RUN tar xvzf flink-1.7.1-bin-hadoop27-scala_2.11.tgz
COPY target/scala-2.11/flink-playground-assembly-0.1-SNAPSHOT.jar flink-playground.jar

WORKDIR /flink-1.7.1/bin
ENTRYPOINT ["./start-cluster.sh", "./flink run /flink-playground.jar"]