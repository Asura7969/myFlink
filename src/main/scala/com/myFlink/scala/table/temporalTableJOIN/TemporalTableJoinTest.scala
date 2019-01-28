package com.myFlink.scala.table.temporalTableJOIN

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

/**
  * Without connnector 实现代码
  */
object TemporalTableJoinTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    // 设置时间类型是 event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 构造订单数据
    val ordersData = new mutable.MutableList[(Long, String, Timestamp)]
    ordersData.+=((2L, "Euro", new Timestamp(2L)))
    ordersData.+=((1L, "US Dollar", new Timestamp(3L)))
    ordersData.+=((50L, "Yen", new Timestamp(4L)))
    ordersData.+=((3L, "Euro", new Timestamp(5L)))

    //构造汇率数据
    val ratesHistoryData = new mutable.MutableList[(String, Long, Timestamp)]
    ratesHistoryData.+=(("US Dollar", 102L, new Timestamp(1L)))
    ratesHistoryData.+=(("Euro", 114L, new Timestamp(1L)))
    ratesHistoryData.+=(("Yen", 1L, new Timestamp(1L)))
    ratesHistoryData.+=(("Euro", 116L, new Timestamp(5L)))
    ratesHistoryData.+=(("Euro", 119L, new Timestamp(7L)))

    // 进行订单表 event-time 的提取
    val orders = env
      .fromCollection(ordersData)
      .assignTimestampsAndWatermarks(new OrderTimestampExtractor[Long, String]())
      .toTable(tEnv, 'amount, 'currency, 'rowtime.rowtime)

    // 进行汇率表 event-time 的提取
    val ratesHistory = env
      .fromCollection(ratesHistoryData)
      .assignTimestampsAndWatermarks(new OrderTimestampExtractor[String, Long]())
      .toTable(tEnv, 'currency, 'rate, 'rowtime.rowtime)

    // 注册订单表和汇率表
    tEnv.registerTable("Orders", orders)
    tEnv.registerTable("RatesHistory", ratesHistory)
    val tab = tEnv.scan("RatesHistory")
    // 创建TemporalTableFunction
    val temporalTableFunction = tab.createTemporalTableFunction('rowtime, 'currency)
    //注册TemporalTableFunction
    tEnv.registerFunction("Rates",temporalTableFunction)

    val SQLQuery =
      """
        |SELECT o.currency, o.amount, r.rate,
        |  o.amount * r.rate AS yen_amount
        |FROM
        |  Orders AS o,
        |  LATERAL TABLE (Rates(o.rowtime)) AS r
        |WHERE r.currency = o.currency
        |""".stripMargin

    tEnv.registerTable("TemporalJoinResult", tEnv.sqlQuery(SQLQuery))

    val result = tEnv.scan("TemporalJoinResult").toAppendStream[Row]
    // 打印查询结果
    result.print()
    env.execute()
  }

}

class OrderTimestampExtractor[T1, T2]
  extends BoundedOutOfOrdernessTimestampExtractor[(T1, T2, Timestamp)](Time.seconds(10)) {
  override def extractTimestamp(element: (T1, T2, Timestamp)): Long = {
    element._3.getTime
  }
}
