package com.myFlink.scala.sql

import java.io.{File, FileOutputStream, OutputStreamWriter}

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources._
import org.apache.flink.types.Row

/**
  * 期望的结果:
  *
  * Mike,1,12.3,Smith
  * Bob,2,45.6,Taylor
  * Sam,3,7.89,Miller
  * Peter,4,0.12,Smith
  * Liz,5,34.5,Williams
  * Sally,6,6.78,Miller
  * Alice,7,90.1,Smith
  * Kelly,8,2.34,Williams
  */
object CsvSourceSinkDemo {
  def main(args: Array[String]): Unit = {
    // Streaming 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    //方便我们查出输出数据
    env.setParallelism(1)

    val sourceTableName = "csvSource"
    // 创建CSV source数据结构
    val tableSource = getCsvTableSource

    val sinkTableName = "csvSink"
    // 创建CSV sink 数据结构
    val tableSink = getCsvTableSink

    // 注册source
    tEnv.registerTableSource(sourceTableName, tableSource)
    // 注册sink
    tEnv.registerTableSink(sinkTableName, tableSink)

    val sql = "SELECT * FROM csvSource "
    // 执行查询
    val result = tEnv.sqlQuery(sql)

    // 将结果插入sink
    result.insertInto(sinkTableName)
    env.execute()
  }

  def getCsvTableSink: TableSink[Row] = {
    val tempFile = File.createTempFile("csv_sink_", "tem")
    println("Sink path : " + tempFile)
    if (tempFile.exists()) {
      tempFile.delete()
    }
    new CsvTableSink(tempFile.getAbsolutePath).configure(
      Array[String]("first", "id", "score", "last"),
      Array[TypeInformation[_]](Types.STRING, Types.INT, Types.DOUBLE, Types.STRING))
  }


  def getCsvTableSource: CsvTableSource = {
    val csvRecords = Seq(
      "First#Id#Score#Last",
      "Mike#1#12.3#Smith",
      "Bob#2#45.6#Taylor",
      "Sam#3#7.89#Miller",
      "Peter#4#0.12#Smith",
      "% Just a comment",
      "Liz#5#34.5#Williams",
      "Sally#6#6.78#Miller",
      "Alice#7#90.1#Smith",
      "Kelly#8#2.34#Williams"
    )
    // 测试数据写入临时文件
    val tempFilePath = writeToTempFile(csvRecords.mkString("$"), "csv_sink_", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("first", "id", "score", "last"),
      Array(
        Types.STRING,
        Types.INT,
        Types.DOUBLE,
        Types.STRING
      ),
      fieldDelim = "#",
      rowDelim = "$",
      ignoreFirstLine = true,
      ignoreComments = "%"
    )
  }

  private def writeToTempFile(contents: String,
                              filePrefix: String,
                              fileSuffix: String,
                              charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    tempFile.getAbsolutePath
  }
}
