package com.myFlink.scala.batch

import com.myFlink.java.utils.WordCountData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._

object WorldCount {

  val INPUT_KEY:String = "input"
  val OUTPUT_KEY:String = "output"

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)

    val text =
      if(params.has(INPUT_KEY)){
        env.readTextFile(params.get(INPUT_KEY))
      }else{
        env.fromCollection(WordCountData.WORDS)
      }

    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)

    if(params.has(OUTPUT_KEY)){
      counts.writeAsCsv(params.get(OUTPUT_KEY),"\n"," ")
      env.execute("Scala WordCount")
    }else{
      counts.print()
    }
  }
}
