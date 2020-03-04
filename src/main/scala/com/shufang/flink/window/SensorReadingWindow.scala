package com.shufang.flink.window

import com.shufang.flink.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SensorReadingWindow {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //指定时间戳的的定义为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    env.getConfig.setAutoWatermarkInterval(100)

    val inputstream: DataStream[SensorReading] = env.socketTextStream("localhost", 9999)
      .map {
        line =>
          val words: Array[String] = line.split(",")
          SensorReading(words(0), words(1).toLong, words(2).toDouble)
      }

    /**
     * waterMark的指定必须在keyby操作之前，一般是在dataStream生成之后立即引入watermark
     * 或者在map\filter操作之后指定watermark
     */
    val wstream: DataStream[SensorReading] = inputstream.assignTimestampsAndWatermarks(
      //允许数据lateness一秒，当时间戳为9的数据到了之后，watermark变成(9-1),8及8以前10秒内窗口的数据进行输出，然后9s的窗口开始，
      //等19s的时候watermark变成18，18及18以前的数据再输出
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.temperture.toLong
        }
      }
    )

    val outputStream: DataStream[(String, Double)] = wstream.map(a => (a.id, a.temperture))
      .keyBy(_._1)
      .timeWindow(Time.seconds(8))
      .reduce((a, b) => (a._1, a._2.max(b._2)))


    outputStream.print("output")

    wstream.print("input")


    env.execute("sensor reading window opers ")
  }
}
