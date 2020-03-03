package com.shufang.flink.operator

import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object ConnectAndCoMapApi {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val nums: DataStream[Int] = env.fromCollection(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val strings: DataStream[String] = env.readTextFile("src/main/resources/flinkFlile")


    // 会形成一个ConnectedStream
    val coStream: ConnectedStreams[Int, String] = nums.connect(strings)

    // ConnectedStream.map最终返回一个DataStream[公共父类]
    val resultStream: DataStream[AnyRef] = coStream.map(
      //可以传入2个转换函数，分别操作之前的不同数据类型
      (0, _),
      (_, 1)
    )

    resultStream.print()


    /**
     * union 操作
     */
    val nums2: DataStream[Int] = env.fromCollection(List(1,2,3,4,5,6,7,8,9,10))
    val nums3: DataStream[Int] = env.fromCollection(Seq(1,2,3,4,5,6,7,8,9,10))
    val numsAll: DataStream[Int] = nums.union(nums2).union(nums3)
    numsAll.print()

    env.execute("coMap")


  }
}
