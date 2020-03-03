package com.shufang.flink.connectors

import com.shufang.flink.bean.People
import com.shufang.flink.examples.MyPeopleSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * 首先引入Apache Bahir的Redis连接器依赖,这里主要是测试Redis的Sink功能
 * <!-- https://mvnrepository.com/artifact/org.apache.bahir/flink-connector-redis -->
 * <dependency>
 * <groupId>org.apache.bahir</groupId>
 * <artifactId>flink-connector-redis_2.11</artifactId>
 * <version>1.0</version>
 * </dependency>
 */
object TestRedisSink {

  def main(args: Array[String]): Unit = {

    /**
     * 一般的SinkFunction不是继承 RichSinkFunction 就是 impl SinkFunction
     * public class RedisSink<IN> extends RichSinkFunction<IN> {
     */

    //获取连接环境
    val envi: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //获取数据源
    val peoples: DataStream[People] = envi.addSource(new MyPeopleSource())

    // 创建一个redis连接池的配置
    val jedisPoolConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()


    //然后配置一个redis的mapper,实现里面的3个方法，具体可以参考官网

    class MyMapper extends RedisMapper[People] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME")
      }

      override def getKeyFromData(data: People): String = {
        data.id.toString
      }

      override def getValueFromData(data: People): String = {
        data.name + "-" + data.score
      }
    }

    //创建Sink
    peoples.addSink(new RedisSink[People](
      jedisPoolConfig,
      new MyMapper
    ))

    envi.execute()
  }
}
