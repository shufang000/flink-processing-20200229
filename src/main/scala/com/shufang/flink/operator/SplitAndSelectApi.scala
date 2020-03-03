package com.shufang.flink.operator

import com.shufang.flink.bean.People
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * 本类主要是测试split、select算子的分流操作
 * split操作在1.7.2中已经被deprecated了，建议用底层的side output方式代替
 */
object SplitAndSelectApi {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val buffer: ArrayBuffer[(Int, String, Double)] = ArrayBuffer[(Int, String, Double)]()
    val random = new Random()

    for (elem <- 1 to 15) {
      val tuple: (Int, String, Double) = (random.nextInt(10), "people_" + elem, random.nextInt(100))
      buffer.append(tuple)
    }


    val tuples: DataStream[(Int, String, Double)] = env.fromCollection(buffer)

    val peoples: DataStream[People] = tuples.map(
      tuple =>
        People(tuple._1, tuple._2, tuple._3)
    )

    /**
     * 用split、select算子
     */
    val splitStream: SplitStream[People] = peoples.split(
      people =>
        if (people.id > 8) Seq("high") else Seq("low")
    )
    val highStream: DataStream[People] = splitStream.select("high")
    val lowStream: DataStream[People] = splitStream.select("low")

    highStream.print("high").setParallelism(1)

    lowStream.print("low").setParallelism(1)

    env.execute("split")
  }
}
