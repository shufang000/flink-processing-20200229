package com.shufang.flink.examples

import com.shufang.flink.bean.People
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._

/**
 * 用来测试我自定义的Source：MyPeopleSource
 */
object StreamFromMyPeopleSource {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val people: DataStream[People] = env.addSource(new MyPeopleSource())

    people.print()

    env.execute("myPeopleSource")

  }
}
