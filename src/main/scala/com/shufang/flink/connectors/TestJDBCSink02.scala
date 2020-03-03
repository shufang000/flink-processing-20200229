package com.shufang.flink.connectors

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.shufang.flink.bean.People
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * case class People(id: Int, name: String, score: Double)
 */
object TestJDBCSink02 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val tuples: Seq[(Int, String, Double)] = Seq(
      (1001, "shufang", 100),
      (1002, "yangtao", 98.12),
      (1003, "lanyage", 99.99)
    )

    val pStream: DataStream[People] = env.fromCollection(tuples).map(a => People(a._1, a._2, a._3))

    pStream.addSink(new MyJdbcFuntion())

    val poolConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()

    class MyMapper() extends RedisMapper[People] {
      //设置存进去的数据格式是HASH还是LIST还是ZSET命令：RedisCommand是一个JAVA中的枚举类，属性全是redis的调用方法
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "peoples")
      }

      //设置key的
      override def getKeyFromData(data: People): String = {
        data.id + "-" + data.name
      }

      //设置存进去的value
      override def getValueFromData(data: People): String = {
        data.score.toString
      }
    }

    pStream.addSink(
      new RedisSink(poolConfig, new MyMapper()))

    env.execute("jdbc sink to")
  }
}


class MyJdbcFuntion() extends RichSinkFunction[People] {

  val URL = "jdbc:mysql://localhost:3306/flink_jdbc"
  val USERNAME = "root"
  val PASSWORD = "888888"

  var insertStatment: PreparedStatement = null
  var updateStatment: PreparedStatement = null
  var conn: Connection = null

  override def invoke(value: People, context: SinkFunction.Context[_]): Unit = {

    updateStatment.setString(1, value.name)
    updateStatment.setInt(2, value.id)
    updateStatment.execute()

    if (updateStatment.getUpdateCount == 0) {
      insertStatment.setString(1, value.name)
      insertStatment.setDouble(2, value.score)
      insertStatment.execute()
    }
  }

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)
    insertStatment = conn.prepareStatement("insert into people(name,score) values(?,?) ", 1)
    updateStatment = conn.prepareStatement("update people set name = ? where id = ?")
  }

  override def close(): Unit = {
    insertStatment.close()
    updateStatment.close()
    conn.close()
  }
}







