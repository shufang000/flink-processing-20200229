package com.shufang.flink.functions

import com.shufang.flink.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutputDemo {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[SensorReading] = env.socketTextStream("localhost", 9999).map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timeStamp
      }
    })


    // 处理后得到的是主流
    val mainStream: DataStream[SensorReading] = inputStream.process(new ColdFunction())

    // 通过主流得到侧输出流
    val sideStream: DataStream[String] = mainStream.getSideOutput(new OutputTag[String]("cold flag"))


    mainStream.print("主流")
    sideStream.print("侧输出流")
    env.execute()
  }
}

class ColdFunction() extends ProcessFunction[SensorReading, SensorReading] {

  private lazy val outputlag = new OutputTag[String]("cold flag")

  override def processElement(
                               value: SensorReading,
                               ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {
    if (value.temperture < 30) {
      ctx.output(outputlag, "cold报警" +value.id+"\t"+ value.temperture)
    }else{
      out.collect(value)
    }
  }
}
