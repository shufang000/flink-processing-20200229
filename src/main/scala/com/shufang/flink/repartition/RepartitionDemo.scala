package com.shufang.flink.repartition

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

/**
 * rebalance
 * re
 */
object RepartitionDemo {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.socketTextStream("localhost", 9999)

    stream.rescale
    stream.rebalance
    stream.shuffle
    stream.partitionCustom(new MyPartitioner(),0)
    //    println(stream.executionConfig.getParallelism)


    //    env.execute("repatitionDemo")
  }

}

class MyPartitioner() extends  Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
    key.hashCode
  }
}
