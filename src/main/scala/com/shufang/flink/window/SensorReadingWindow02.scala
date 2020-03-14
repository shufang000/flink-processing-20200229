package com.shufang.flink.window

import com.shufang.flink.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object SensorReadingWindow02 {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //为了好测试，将并行度设为1
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val sensorStream: DataStream[SensorReading] = env.socketTextStream("localhost", 9999)
      .map(a => {
        val strings: Array[String] = a.split(",")
        SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
      })


    val value: DataStream[SensorReading] = sensorStream.assignTimestampsAndWatermarks(new MyAssigner)

    val output: DataStream[(String, Double)] = value.map {
      s =>
        (s.id, s.temperture)
    }.keyBy(0)
      .timeWindow(Time.seconds(10))
      .reduce(
        (t1, t2) =>
          (t1._1, t1._2.max(t2._2))
      )
    output.print("output")

    sensorStream.print("input")


    env.execute("sensor reading")
  }
}

class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  //这个就是延迟时间，设为1秒
  val lateness = 1000

  //用来接受最大的Timestamp
  var maxTs = Long.MinValue


  override def getCurrentWatermark: Watermark = {
    //watermark=当前进入的最大timestamp -1
    new Watermark(maxTs - lateness)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(System.currentTimeMillis())
    System.currentTimeMillis()
  }
}
