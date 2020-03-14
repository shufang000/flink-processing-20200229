package com.shufang.flink.executionconfig

import com.esotericsoftware.kryo.serializers.DefaultSerializers.KryoSerializableSerializer
import com.shufang.flink.bean.People
import org.apache.flink.api.common.{ExecutionConfig, ExecutionMode}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ExecutionConfigs {
  def main(args: Array[String]): Unit = {

    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //配置常用网址
    //https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/execution_configuration.html
    //所有的executionConfig都可以通过 env.method()进行设定，底层是通过javaEnv.setConfig() ->  config.setxxxConfig()





    env.execute()
  }
}
