package com.shufang.flink.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ArrayBuffer

/**
 * 这个案例只是在本地模式下测试单词计数
 */
object FlinkWordCountFromSocket {
  def main(args: Array[String]): Unit = {

    /**
     * 从args中获取传参数
     */


    val host: String = "localhost"
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("no port specified. default port is 9999")
      }
        9999
    }
    println(port)
    println(host)
    /**
     * 使用有界流来处理wordcount
     */
    //获取有界流运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //模拟一批数据
    val words: ArrayBuffer[String] = ArrayBuffer("a", "a", "a", "b", "b", "c", "c", "c", "a")

    //这里需要注意implicit转换，所有需要引入依赖：org.apache.flink.api.scala._
    val ds: DataSet[String] = env.fromCollection(words)

    //这是使用有界流饿方式进行wordcount
    val counts: AggregateDataSet[(String, Int)] = ds.map((_, 1)).groupBy(0).sum(1)

    counts.print()


    /**
     * 使用无界流来处理wordcount
     */

    val env1: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //可以设置并行度
    env1.setParallelism(5)

    //肩痛socket端口处理数据
    val dstream: DataStream[String] = env1.socketTextStream(host, port, '\n')

    val result: DataStream[(String, Int)] = dstream
      .flatMap(_.split(" ")).setParallelism(2)
      .filter(_.nonEmpty).setParallelism(2)
      .map((_, 1)).setParallelism(2)
      .keyBy(0)
      .reduce((a, b) => (a._1, a._2 + b._2))


    result.print()

    env1.execute("stream_wordcount")


  }
}
