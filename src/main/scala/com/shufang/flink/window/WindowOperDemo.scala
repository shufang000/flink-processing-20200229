package com.shufang.flink.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowOperDemo {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1.设置时间特性，不设置的话默认就是ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val ds: DataStream[String] = env.socketTextStream("localhost", 9999)

    val mapStream: DataStream[(String, Int)] = ds.map((_, 1)).assignAscendingTimestamps(_ =>System.currentTimeMillis())


    val countStream: DataStream[(String, Int)] = mapStream
      .keyBy(0)
      .sum(1)

    countStream.keyBy(0).timeWindow(Time.seconds(5), Time.seconds(3)).max(1).print("max count data")
    println("------------------------------")
    countStream.print("input data")


    env.execute()
  }
}
