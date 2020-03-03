package com.shufang.flink.examples

import com.shufang.flink.bean
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random
import com.shufang.flink.bean.People
/**
 * 这个类主要是用来模拟数据，生成自定义source
 *
 * @PublicEvolving
 * 可以按照以下Source自定义Source
 * public class SocketTextStreamFunction implements SourceFunction<String>
 * SourceFunction是一个java接口，但是在scala中不能impl只能extends
 */
class  MyPeopleSource extends SourceFunction[People] {

  //定义一个控制Source开关的变量
  var isRunning: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[People]): Unit = {

    //定义以及随机数生成器
    val random = new Random()

    //用来模拟不同人的不同分数
    val current_score = 1.to(10).map {
      case i =>
        ("people_" + i, 60 + random.nextGaussian() * 20)
    }


    //用无限循环来生成数据流
    while (isRunning) {

      current_score.foreach(
        ps => {
          val name: String = ps._1
          val score: Double = ps._2

          val people:bean.People = People(random.nextInt(10),name,score)

          ctx.collect(people)
        }
      )

      //模拟数据生成间隔
      Thread.sleep(1500)

    }
  }

  /**
   * 表示在isRunning为false时，source关闭
   */
  override def cancel(): Unit = {
    isRunning = false
  }


}

