package com.shufang.flink.state

import com.shufang.flink.bean.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object StateDemo {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(300)
    //    env.setStateBackend(new MemoryStateBackend())

    val sensorStream: DataStream[SensorReading] = env.socketTextStream("localhost", 9999)
      .map(a => {
        val strings: Array[String] = a.split(",")
        SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timeStamp
      }
    })

    val processedStream: DataStream[(String, Double, Double)] = sensorStream.keyBy(_.id)
      .process(new MyProcessFunction01)

    val processedStream02: DataStream[(String, Double, Double)] = sensorStream.keyBy(_.id)
      .flatMap(new MyFlatMapFunction)

    /**
     * 1.通过flatMapWithstate的方式来保存状态 Seq与List都是TraversableOnce类型
     * 这里面的状态其实就是通过Option（）来维护的，
     * 如果之前没有状态保存，那么option就是None，
     * 吐过之前有保存过状态，那么Option就是Some，可以通过getOrElse（0）获取状态
     * -----------------------------------------------------------------------
     * def flatMapWithState[R: TypeInformation, S: TypeInformation](
     * fun: (T, Option[S]) => (TraversableOnce[R], Option[S])): DataStream[R]
     * 源码范型解析：
     * >>>>>>>> DataStream[R] -> R就是返回值中的类型
     * >>>>>>>> Oprion[S] -> 那么S肯定就是状态的类型
     * >>>>>>>> 返回值、状态类型都有了，那么T肯定就是输入数据的类型
     */
    val processStream03: DataStream[(String, Double, Double)] = sensorStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
        //如果状态为空，那么只更新state为当前temp
        case (sensor: SensorReading, None) => (List.empty, Some(sensor.temperture))
        //实际上，这里使用Option来维持状态的，没有状态保存而获取的话，就相当于getorelse(0)

        case (sensor: SensorReading, pretemp: Some[Double]) =>
          val lastTemp: Double = pretemp.get
          val diff: Double = (sensor.temperture - lastTemp).abs
          if (diff > 10) {
            (Seq((sensor.id, lastTemp, sensor.temperture)), Some(sensor.temperture))
          } else {
            (List(), Some(sensor.temperture))
          }
      }
    processStream03.print("flatMapWith-result")
    //    processedStream02.print("报警信息-温度波动过大")
    sensorStream.print("输入数据")

    env.execute("state")
  }
}


/**
 * 2.flatMapFunction
 */
class MyFlatMapFunction extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var preTemp: ValueState[Double] = _
  //利用open函数的特性，在初始化的时候就执行
  override def open(parameters: Configuration): Unit = {
    preTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading,
                       out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp: Double = preTemp.value()

    if ((value.temperture - lastTemp).abs > 10) {
      out.collect((value.id, lastTemp, value.temperture))
    }
    preTemp.update(value.temperture)
  }

}


/**
 * 3.processFunction
 * processFunction可以处理所有Api能处理的事情
 * 主要方法processElement(ctx,value,out)、onTime(ctx,value,out)回调函数
 */
class MyProcessFunction01 extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
  //声明State
  lazy val pretemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("pretemp", classOf[Double]))

  override def processElement(
                               value: SensorReading,
                               ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context,
                               out: Collector[(String, Double, Double)]): Unit = {
    //调用state
    val lastTemp: Double = pretemp.value()
    val currentTemp: Double = value.temperture

    if ((currentTemp - lastTemp).abs > 10) {
      out.collect((value.id, lastTemp, currentTemp))
    }

    //更新state
    pretemp.update(currentTemp)

  }
}
