package com.myFlink.stream

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.concurrent.ExecutionContext

object AsynSource {
  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "xxx:9092,xxx:9092,xxx:9092")
    properties.setProperty("zookeeper.connect", "xxx:2181,xxx:2181,xxx:2181")
//    properties.setProperty("zookeeper.connect", "xxx:2185,xxx:2185")
    properties.setProperty("group.id", "groupId")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(5000)

    val kafkaConsumer = new FlinkKafkaConsumer010(
      "topicName",
      new SimpleStringSchema,
      properties)

    val messageStream: DataStream[String] = env.addSource(kafkaConsumer)

    val asyncStream: DataStream[(String,util.List[String])] =
      AsyncDataStream.unorderedWait(messageStream,new AsyncDatabaseRequest(),1000L,TimeUnit.MILLISECONDS,100)

    val value = asyncStream.map(x => {
      x._2.add(x._1).toString
    }).filter(_ != null)

    value.addSink(initSinkKafka)

    env.execute("flinkStream")
  }

  def initSinkKafka:FlinkKafkaProducer010[String] = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "xxx:9092,xxx:9092,xxx:9092")
    properties.setProperty("zookeeper.connect", "xxx:2181,xxx:2181,xxx:2181")
    properties.setProperty("group.id", "groupId")

    val kafkaProducer: FlinkKafkaProducer010[String] = new FlinkKafkaProducer010(
      "topicName",
      new SimpleStringSchema,
      properties)

    kafkaProducer
  }

  class AsyncDatabaseRequest extends AsyncFunction[String, (String,util.List[String])] {

    lazy val pool: JedisPool = new JedisPool(new JedisPoolConfig, "ip", 6379, 2000, "123456", 0, "clientName")

    /** The context used for the future callbacks */
    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

    override def asyncInvoke(str: String, resultFuture: ResultFuture[(String,util.List[String])]): Unit = {

      val jedis = pool.getResource
      val keys: util.List[String] = jedis.lrange("key", 0, -1)
      jedis.close()

      val result = Iterable((str,keys))
      resultFuture.complete(result)
    }
  }
}
