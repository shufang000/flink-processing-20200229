package com.shufang.flink.functions

import com.shufang.flink.bean.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 假如连续
 */
object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorStream: DataStream[SensorReading] = env.socketTextStream("localhost", 9999).map(
      line => {
        val strings: Array[String] = line.split(",")

        SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
      }
      //设置Watermark
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timeStamp
      }
    })


    val alertStream: DataStream[String] = sensorStream.keyBy(_.id)
      .process(new MyTemptrueAlert())

    sensorStream.print("input")

    alertStream.print("output")




    env.execute("process function")
  }
}


/** 自定义报警的processFuntion，如果连续温度上升，就报警String类型的数据
 * * @param <K> keyby的key的类型
 * * @param <I> dataStream的输入类型，这里是SensorReading
 * * @param <O> 输出的类型，这里是String类型的温度报警类型
 * * @KeyedProcessFunction<K, I, O> extends AbstractRichFunction
 */
class MyTemptrueAlert extends KeyedProcessFunction[String, SensorReading, String] {

  /**
   * 定义一个属性保存上个温度的状态，这个状态每次与当前状态进行对比之后，最后用当前的温度更新这个状态
   * public ValueStateDescriptor(String name, Class<T> typeClass) {
   * super(name, typeClass, null) ;
   * }
   */
  lazy val preTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("preTemp", classOf[Double]))
  //定义一个状态，用来保存需要被触发的定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("curentTimer", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //先取出上一个温度值
    val lastTemp: Double = preTemp.value()

    //更新温度值
    preTemp.update(value.temperture)

    // 获取当前的Timer的触发的timestamp
    val curTimerTs: Long = currentTimer.value()

    //如果温度连续上升，而且之前没有注册过Timer，那么我们就注册 Timer定时器
    if (lastTemp < value.temperture && curTimerTs == 0) {

      // 注册一个定时器，并设置触发时间，触发时间是当前时间后1s
      val timerTs: Long = ctx.timerService().currentProcessingTime() + 1000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)

      // 注册之后更新这个timer的ts状态
      currentTimer.update(timerTs)

    }
    //
    else if (lastTemp > value.temperture || lastTemp == 0.0) {

      //如果温度下降、或者第一条数据，删除定时器、清空定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }

  /**
   * 这个方法主要是用来触发Timer时调用回调方法
   * @param timestamp
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      //在报警后我么只需要打印报警信息就好了
    println(ctx.getCurrentKey + "高温报警,请注意")
  }
}
