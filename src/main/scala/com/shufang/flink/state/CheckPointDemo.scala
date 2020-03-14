package com.shufang.flink.state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CheckPointDemo {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(60000)
    //env.getCheckpointConfig.setCheckpointInterval()
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //.......这里可以有多种checkpoint的设置

    //    env.setStateBackend()

    /**
     * 指定从checkpoint重启策略
     * 1。Fixed Delay Restart Strategy
     * restart-strategy.fixed-delay.attempts: 3
     * restart-strategy.fixed-delay.delay: 10 s
     *
     * 2。Failure Rate Restart Strategy
     * restart-strategy.failure-rate.max-failures-per-interval: 3
     * restart-strategy.failure-rate.failure-rate-interval: 5 min
     * restart-strategy.failure-rate.delay: 10 s
     *
     * 3.No Restart Strategy
     * restart-strategy: none
     *
     * 4。Fallback Restart Strategy(自动选择重启策略，默认为1。Fixed Delay Restart Strategy，非常有用)
     * The cluster defined restart strategy is used. This helpful for streaming programs which enable checkpointing.
     * Per default, a fixed delay restart strategy is chosen if there is no other restart strategy defined.
     */

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, // number of restart attempts
      Time.of(10, TimeUnit.SECONDS) // delay
    ))

    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      3, // 允许失败3次
      Time.of(5, TimeUnit.MINUTES), // 5分钟内允许失败3次
      Time.of(10, TimeUnit.SECONDS) // 没10s尝试重启一次
    ))




    //TODO 转换逻辑


    env.execute("checkpoint ")
  }
}
