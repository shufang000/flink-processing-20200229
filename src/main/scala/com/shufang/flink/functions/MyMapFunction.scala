package com.shufang.flink.functions

import com.shufang.flink.bean.People
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration

class MyMapFunction  extends  RichMapFunction[People,String]{
  override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

  override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

  override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  override def map(value: People): String = ???
}
