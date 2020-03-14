package com.shufang.flink.connectors

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaSource01 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //这是checkpoint的超时时间
    //env.getCheckpointConfig.setCheckpointTimeout()
    //设置最大并行的chekpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(5)
    env.getCheckpointConfig.setCheckpointInterval(1000) //增加checkpoint的中间时长，保证可靠性


    /**
     * 为了保证数据的一致性，我们开启Flink的checkpoint一致性检查点机制，保证容错
     */
    env.enableCheckpointing(60000)

    /**
     * 从kafka获取数据，一定要记得添加checkpoint，能保证offset的状态可以重置，从数据源保证数据的一致性
     * 保证kafka代理的offset与checkpoint备份中保持状态一致
     */

    val kafkaonfigs = new Properties()

    //指定kafka的启动集群
    kafkaonfigs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    //指定消费者组
    kafkaonfigs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flinkConsumer")
    //指定key的反序列化类型
    kafkaonfigs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    //指定value的反序列化类型
    kafkaonfigs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    //指定自动消费offset的起点配置
    //    kafkaonfigs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")


    /**
     * 自定义kafkaConsumer，同时可以指定从哪里开始消费
     * 开启了Flink的检查点之后，我们还要开启kafka-offset的检查点，通过kafkaConsumer.setCommitOffsetsOnCheckpoints(true)开启，
     * 一旦这个检查点开启，那么之前配置的 auto-commit-enable = true的配置就会自动失效
     */
    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "console-topic",
      new SimpleStringSchema(), // 这个schema是将kafka的数据应设成Flink中的String类型
      kafkaonfigs
    )

    // 开启kafka-offset检查点状态保存机制
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)

    //    kafkaConsumer.setStartFromEarliest()//
    //    kafkaConsumer.setStartFromTimestamp(1010003794)
    //    kafkaConsumer.setStartFromLatest()
    //    kafkaConsumer.setStartFromSpecificOffsets(Map[KafkaTopicPartition,Long]()

    // 添加source数据源
    val kafkaStream: DataStream[String] = env.addSource(kafkaConsumer)

    kafkaStream.print()

    val sinkStream: DataStream[String] = kafkaStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(5)) {
      override def extractTimestamp(element: String): Long = {
        element.split(",")(1).toLong
      }
    })


    /**
     * 通过FlinkkafkaProduccer API将stream的数据写入到kafka的'sink-topic'中
     */
    //    val brokerList = "localhost:9092"
    val topic = "sink-topic"
    val producerConfig = new Properties()
    producerConfig.put(ProducerConfig.ACKS_CONFIG, new Integer(1)) // 设置producer的ack传输配置
    producerConfig.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Time.hours(2)) //设置超市时长，默认1小时，建议1个小时以上

    /**
     * 自定义producer，可以通过不同的构造器创建
     */
    val producer: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
      topic,
      new KeyedSerializationSchemaWrapper[String](SimpleStringSchema),
      producerConfig,
      Semantic.EXACTLY_ONCE
    )

    //    FlinkKafkaProducer.SAFE_SCALE_DOWN_FACTOR
    /** *****************************************************************************************************************
     * * 出了要开启flink的checkpoint功能，同时还要设置相关配置功能。
     * * 因在0.9或者0.10，默认的FlinkKafkaProducer只能保证at-least-once语义，假如需要满足at-least-once语义，我们还需要设置
     * * setLogFailuresOnly(boolean)    默认false
     * * setFlushOnCheckpoint(boolean)  默认true
     * * come from 官网 below：
     * * Besides enabling Flink’s checkpointing，you should also configure the setter methods setLogFailuresOnly(boolean)
     * * and setFlushOnCheckpoint(boolean) appropriately.
     * ******************************************************************************************************************/

    producer.setLogFailuresOnly(false) //默认是false


    /**
     * 除了启用Flink的检查点之外，还可以通过将适当的semantic参数传递给FlinkKafkaProducer011（FlinkKafkaProducer对于Kafka> = 1.0.0版本）
     *
     * 来选择三种不同的操作模式：
     * Semantic.NONE  代表at-mostly-once语义
     * Semantic.AT_LEAST_ONCE（Flink默认设置）
     * Semantic.EXACTLY_ONCE：使用Kafka事务提供一次精确的语义，每当您使用事务写入Kafka时，
     * 请不要忘记为使用Kafka记录的任何应用程序设置所需的设置isolation.level（read_committed 或read_uncommitted-后者是默认值)
     */

    sinkStream.addSink(producer)

    env.execute("kafka source & sink")
  }
}
