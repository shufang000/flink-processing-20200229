package com.shufang.flink.table_and_sql_api

import com.shufang.flink.bean.People
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.StreamTableEnvironment


/**
 * 主要是用来测试TableAPI和Flink SQL
 * case class People(id: Int, name: String, score: Double)
 */
object TableTest {
  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //获取Table环境
    val tEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //获取到一个流，同时将流中的数据转化成样例类
    val peopleStream: DataStream[People] = env.socketTextStream("localhost", 9999).map(
      line => {
        val strings: Array[String] = line.split(",")
        People(strings(0).trim.toInt, strings(1), strings(2).trim.toDouble)
      }
    )

    println(peopleStream.executionConfig.getParallelism)

    //然后通过TableEnv中获取Table  将DataStream 转化成 动态Table、
    val peopleTable: Table = tEnv.fromDataStream(peopleStream)

    //因为是case class People ，tEnv可以根据样例类生成动态表，属性就代表字段
    val table: Table = peopleTable.select("id,name,score").filter("score > 80  ")
    val table1: Table = tEnv.sqlQuery("select id ,count(id) from " + table )


//    tEnv.toRetractStream(table1).print()

    /**
     * 将动态表转化成Stream
     * 方式1: tEnv.toAppendStream[(Int, String, Double)](table)
     * 方式2: tEnv.toRetractStream[People](peopleTable)
     */
    val tableToStream: DataStream[(Int, String, Double)] = tEnv.toAppendStream[(Int, String, Double)](table)
    val tableToStream02: DataStream[(Boolean, People)] = tEnv.toRetractStream[People](peopleTable)

//    tEnv.sqlQuery("select id,name ,score from peopleTable")
    tableToStream.print()

    //执行程序
    env.execute("table test")

  }
}






