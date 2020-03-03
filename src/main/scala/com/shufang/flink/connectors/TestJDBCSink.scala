package com.shufang.flink.connectors

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.shufang.flink.bean.People
import com.shufang.flink.examples.MyPeopleSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * 这个类主要是用来测试Flink通过JDBC-Connector的方式将数据写到MySQL中
 * 由于官方是没有jdbc-connector的，所以我们需要自定义一个Sink
 */
object TestJDBCSink {

  def main(args: Array[String]): Unit = {


    //获取连接环境
    val envi: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //获取数据源
    val peoples: DataStream[People] = envi.addSource(new MyPeopleSource())


    peoples.addSink(new MyJDBCSink())

    envi.execute("jdbc-connector")

    //指定Sink的范型People
    class MyJDBCSink() extends RichSinkFunction[People] {
      //定义SQL的连接、预编译器,给定初始值占位符
      var conn: Connection = _
      var insertStatment: PreparedStatement = _
      var updateStatment: PreparedStatement = _

      /**
       * open()在是初始化的时候就调用的方法，我们直接在初始化的时候创建连接与预编译
       *
       * @param parameters
       */
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_jdbc", "root", "888888")

        insertStatment = conn.prepareStatement("insert into people(name,score) values (?,?)")
        updateStatment = conn.prepareStatement("update people set name = ? where id = ?")
      }

      /**
       * 调用方法
       *
       * @param value
       * @param context
       */
      override def invoke(value: People, context: SinkFunction.Context[_]): Unit = {
        //执行update SQL
        updateStatment.setString(1, value.name)
        updateStatment.setDouble(2, value.id)
        updateStatment.execute()

        //如果update没有查到数据，我们就执行insert SQL
        if (updateStatment.getUpdateCount == 0) {
          insertStatment.setNString(1, value.name)
          insertStatment.setDouble(2, value.score)
          insertStatment.execute()
        }
      }

      /**
       * close方法用来关闭资源，清理工作
       */
      override def close(): Unit = {
        insertStatment.close()
        updateStatment.close()
        conn.close()
      }
    }


  }
}
