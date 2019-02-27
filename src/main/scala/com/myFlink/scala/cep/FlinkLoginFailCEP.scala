package com.myFlink.scala.cep

import com.myFlink.java.cep.LoginWarning
import com.myFlink.java.cep.login.{LoginEvent, LoginWarning}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object FlinkLoginFailCEP {

  val FAIL = "fail"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val loginEventStream = env.fromCollection(List(
      new LoginEvent("1", "192.168.0.1", "fail"),
      new LoginEvent("1", "192.168.0.2", "fail"),
      new LoginEvent("1", "192.168.0.3", "fail"),
      new LoginEvent("2", "192.168.10,10", "success")
    ))

    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(e => FAIL.equals(e.getType))
      .next("next")
      .where(e => FAIL.equals(e.getType))
      .within(Time.seconds(1))

    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    val loginFailDataStream = patternStream
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("next", null).iterator.next()

        new LoginWarning(second.getUserId, second.getIp, second.getType)
      })

    loginFailDataStream.print

    env.execute
  }
}
