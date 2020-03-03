package com.shufang.flink.operator

import com.shufang.flink.bean.People
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object DataStreamOperator {

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
     * 这三种keyBy的方式都是可以的
     * 只是后2种方法不会进行类型推导
     * 记住这里的KeyedStream种的范性是：[原范性,id类型]
     */
    val byPeopleId: KeyedStream[People, Int] = peoples.keyBy(_.id)
    //    peoples.keyBy(0)
    //    peoples.keyBy("id")

    //    byPeopleId.sum("score").print()


    /**
     * 1
     * 3
     * 6
     * 10
     * 15
     * 21
     * 28
     * 36
     * 45
     * 55
     */
    val sumresult: DataStream[Int] = env.fromCollection(1 to 10)
      .map((0,_))
      .keyBy(0)
      .sum(1)
      .map(_._2)

    //能有状态的输出结果
    sumresult.print()

    env.execute("testApi")


  }

}
