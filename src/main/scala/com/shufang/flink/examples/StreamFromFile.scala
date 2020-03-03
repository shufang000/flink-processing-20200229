package com.shufang.flink.examples

import org.apache.flink.streaming.api.scala._

object StreamFromFile {
  def main(args: Array[String]): Unit = {


    //会根据环境推断出应该选择哪个环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.readTextFile("src/main/resources/flinkFlile", "UTF-8").filter(_.nonEmpty)

    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    result.writeAsCsv("/Users/shufang/idea_projects/flink-processing-20200229/src/main/resources/a.csv").setParallelism(1)

    env.execute("fileJob")
  }

}
