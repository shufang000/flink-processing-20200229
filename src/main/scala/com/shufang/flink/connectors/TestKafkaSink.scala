package com.shufang.flink.connectors

import com.shufang.flink.bean.People
import com.shufang.flink.examples.MyPeopleSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * 在Flink中内置了许多其他框架的连接器
 * 一、Bundled Connectors
 * Connectors provide code for interfacing with various third-party systems. Currently these systems are supported:
 *
 * # Apache Kafka (source/sink)
 * Apache Cassandra (sink)
 * Amazon Kinesis Streams (source/sink)
 * # Elasticsearch (sink)
 * # Hadoop FileSystem (sink)
 * RabbitMQ (source/sink)
 * Apache NiFi (source/sink)
 * Twitter Streaming API (source)
 * Google PubSub (source/sink)
 *
 * 二、Connectors in Apache Bahir
 * Additional streaming connectors for Flink are being released through Apache Bahir, including:
 *
 * # Apache ActiveMQ (source/sink)
 * # Apache Flume (sink)
 * # Redis (sink)
 * Akka (sink)
 * Netty (source)
 */
object TestKafkaSink {
  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //获取数据流
    val peopleStream: DataStream[People] = env.addSource(new MyPeopleSource())

    peopleStream.map(_.toString).addSink(
      new FlinkKafkaProducer[String](
        "localhost:9092",
        "console-topic",
        new SimpleStringSchema()
      )
    )

    //执行
    env.execute("kafka_sink")

  }
}
