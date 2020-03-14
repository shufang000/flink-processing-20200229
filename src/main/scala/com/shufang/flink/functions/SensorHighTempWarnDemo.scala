package com.shufang.flink.functions

import com.shufang.flink.bean.SensorReading
import org.apache.flink.api.common.state
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SensorHighTempWarnDemo {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(5000) //设置每5s生成一个watermark，默认是200ms

    val inputStream: DataStream[SensorReading] = env.socketTextStream("localhost", 9999).map(
      line => {

        val strings: Array[String] = line.split(",")

        SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
      }
    )
    /**
     * .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
     * override def extractTimestamp(element: SensorReading): Long = {
     *         element.timeStamp
     * }
     * })
     */

    val keyedStream: KeyedStream[SensorReading, String] = inputStream.keyBy(_.id)

    val outputStream: DataStream[String] = keyedStream.process(new MyProcessFunction())

    inputStream.print("inputstream-is->>>>>")
    outputStream.print("outputStream-is->>>>>>>>>")

    env.execute("test timer")
  }
}

/**
 * KeyedProcessFunction[K,I,O]，其中K是keyby的key的Type、I是input的流元素类型、O是输出的流元素类型
 * K:String 、I:SensorReading 、 O:String
 */
class MyProcessFunction() extends KeyedProcessFunction[String, SensorReading, String] {

  //维护上次传入温度的状态 :getRuntimeContext()
  lazy val lastTemp: state.ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  //用来维护Timer的触发时间
  lazy val currentTimerTs: state.ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimerTs", classOf[Long]))

  /**
   * 用来处理流元素，必须实现的方法！
   *
   * @param value 传入的流元素
   * @param ctx   上下文
   * @param out   输出元素的收集器
   */
  override def processElement(
                               value: SensorReading,
                               ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                               out: Collector[String]): Unit = {
    //首先获取上次的温度:values(),同时更新到当前温度
    val preTemp: Double = lastTemp.value()
    val currentTemp: Double = value.temperture
    lastTemp.update(currentTemp)


    //获取当前注册过的TimerTs的触发时间，如果没有注册过，返回值为0
    val timerTs: Long = currentTimerTs.value()

    //如果当前温度大于之前温度，那么注册Timer
    if (currentTemp > preTemp && timerTs == 0L) {
      //注册Timer，并且将当前的Timer的触发时间状态更新到currentTimerTs
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L)
      currentTimerTs.update(ctx.timerService().currentProcessingTime() + 5000L)

      //如果当前温度比之前状态的温度低
      //或者event还是第一条数据，之前没有维护过temp的state
    } else if (currentTemp < preTemp || preTemp == 0.0d) {
      //清空Timer触发时间的状态，并且删除Timer
      ctx.timerService().deleteProcessingTimeTimer(timerTs)
      currentTimerTs.clear()
    }

  }

  //这是是触发计时器后的回调方法
  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                        out: Collector[String]): Unit = {
    //这里只需要输出相关的报警信息
    out.collect("FBI-WARNING " + ctx.getCurrentKey + timestamp + " 此时高温报警！～")
    currentTimerTs.clear()
  }
}