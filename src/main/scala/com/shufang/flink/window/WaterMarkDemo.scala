package com.shufang.flink.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 每个窗口操作的常规步骤：
 * 1。创建env，并通过env调用setStreamTimeCharacteristic(TimeCharacteristic.EventTime)方法指定程序使用的时间特征
 * 2。[keyby]->window(<指定WindowAssigner如TumblingEventTimeWindows.of(Time.seconds(5)) >)
 * 3。[调用其他可选函数]
 * 4。调用窗口函数reduce........
 * 5。可以选择引入watermark，如果数据有乱序的话
 */
object WaterMarkDemo {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    /**
     * 从调用开始给env创建每一个stream追加时间特征
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val ks: List[String] = "aaaaaaabbbbbbbbbcccc".toList.map(_.toString)

    val kvStream: DataStream[(String, Int)] = env.fromCollection(ks).map((_, 1))


    /**
     * 对于升序数据，我们这样指定：
     *  kvStream.assignAscendingTimestamps(_.timeStamp*1000)
     * 对于乱序数据，我们这样指定
     */
    val withwatermarkStream: DataStream[(String, Int)] = kvStream.assignTimestampsAndWatermarks(
      /**
       * Time.seconds(1) 代表延迟1秒
       * 假如时间戳为5秒的数据来了，那么task的时间戳应该是4，此时只会将4及之前的数据写出去
       */
      new BoundedOutOfOrdernessTimestampExtractor[(String, Int)](Time.seconds(1)) {
        override def extractTimestamp(element: (String, Int)): Long = {
          System.currentTimeMillis()
        }
      }
    )

    /**
     * keyed-window(TimeWindow)
     * 窗口类型确定方式有2种：
     * 1。通过timeWindow()
     * 2。或者通过window(TumplingEventTimeWindow\TumplingProcessingTimeWindow)
     */
    //首先获取一个keyedStream
    val keyedStream: KeyedStream[(String, Int), Tuple] = withwatermarkStream.keyBy(0)

    //调用window方法，指定WindowAssigner()
    //最终返回的是一个dataStream
    //方式2
    val result: DataStream[(String, Int)] = keyedStream.window(
      TumblingEventTimeWindows.of(Time.seconds(5))
    ).reduce((a, b) => (a._1, a._2 + b._2))
    //方式2
    val result1: DataStream[(String, Int)] =
      keyedStream
        .timeWindow(Time.seconds(5))
        .reduce((a, b) => (a._1, a._2 + b._2))

    result1.print()

    env.execute()

  }
}
