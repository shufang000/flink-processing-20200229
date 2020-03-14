package com.shufang.flink.connectors

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object StreamFromKafka {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = new Properties()

    //指定kafka的启动集群
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    //指定消费者组
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flinkConsumer")
    //指定key的反序列化类型
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    //指定value的反序列化类型
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    //指定自动消费的策略
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    println(properties)


    val kafkastream: DataStream[String] = env.addSource(
        new FlinkKafkaConsumer[String]("console-topic",
        new SimpleStringSchema(),
        properties)
    )




//    env.execute("kafkasource")

  }

}
