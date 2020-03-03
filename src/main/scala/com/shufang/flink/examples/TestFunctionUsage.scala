package com.shufang.flink.examples

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ArrayBuffer

object TestFunctionUsage {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[Int] = env.fromCollection(ArrayBuffer(1,2,3,4,5,6,7,8,9,10))


    /**
     * filter方法中需要传一个函数类型的参数，我们有以下几种方式去->传参
     * 1。通过匿名函数类似于: _ > 5
     * 2。通过函数类的形似:
     * class myfilter extends FilterFunction[Int]{
     *    override def filter(value: Int): Boolean = {
     *    value > 5
     *    }
     * }
     *
     * 3。通过变量接收函数的形式
     */
//    stream.filter(_>5)
//    stream.filter(new Myfilter())

    val filterFuntion = (value:Int) => value > 5
    stream.filter(filterFuntion).print()

    env.execute("testFilter")
  }
}

class Myfilter() extends FilterFunction[Int]{
  //实现filter方法
  override def filter(value: Int): Boolean = {
    value > 5
  }
}
