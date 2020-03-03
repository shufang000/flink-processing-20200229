package com.shufang.flink.extension_scala

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._


/**
 * 简单的应用示例->
 * mapWith
 * map (DataStream）
 * data.mapWith {
 * case (_, value) => value.toString
 * }
 *
 * flatMapWith	flatMap (DataStream)
 * data.flatMapWith {
 * case (_, name, visits) => visits.map(name -> _)
 * }
 *
 * filterWith	filter (DataStream)
 * data.filterWith {
 * case Train(_, isOnTime) => isOnTime
 * }
 * ............
 */
object ExtensionMapWith {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val ds: DataStream[(Int, Int)] = env.fromElements((1, 2), (2, 3), (1, 4), (2, 5))

    val value: DataStream[(Int, Int)] = ds.map(
      tuple => {
        val i: Int = tuple._2 + 1
        (tuple._1, i)
      }
    )

    /**
     * mapWith
     */
    val ds1: DataStream[(Int, Int, Int)] = ds.mapWith {
      case (a, b) =>
        (a, b, 1)
    }

    /**
     * filterWith
     */

    ds.filterWith {
      case (a, b) =>
        a == 2
    }.print()


    /**
     * reduceWith
     */
    ds.map(a => ("hello" + a._1, a._2))
        .keyBy(0)
        .reduceWith{
          case ((a,b),(c,d)) =>
            (a+c,b+d)
        }.print()


    env.execute("extension of java")
  }
}



