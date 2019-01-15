package com.myFlink.project

import java.util.Properties

import com.myFlink.project.bean.{ComputeConf, ComputeResult, LogEvent}
import com.myFlink.project.constants.Constants._
import com.myFlink.project.function.{AggregateFunc, ApplyComputeRule}
import com.myFlink.project.schema.{ComputeResultSerializeSchema, LogEventDeserializationSchema}
import com.myFlink.project.source.ConfSource
import com.myFlink.project.watermarker.BoundedLatenessWatermarkAssigner
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}



object Launcher {

  /**
    * @param args:
    * 0: bootstrap Servers
    * 1: groupId
    * 2: consumerTopic
    * 3: retries
    * 4: producerTopic
    * 5: url
    * 6: latency
    */
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /* Checkpoint */
    env.enableCheckpointing(60000L)
    val checkpointConf = env.getCheckpointConfig
    checkpointConf.setMinPauseBetweenCheckpoints(30000L)
    checkpointConf.setCheckpointTimeout(8000L)
    checkpointConf.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /* Kafka consumer */
    val consumerProps = new Properties()
    consumerProps.setProperty(KEY_BOOTSTRAP_SERVERS, args(0))
    consumerProps.setProperty(KEY_GROUP_ID, args(1))
    val consumer = new FlinkKafkaConsumer010[LogEvent](
      args(2),
      new LogEventDeserializationSchema,
      consumerProps
    )

    val producerProps = new Properties()
    producerProps.setProperty(KEY_BOOTSTRAP_SERVERS, args(0))
    producerProps.setProperty(KEY_RETRIES, args(3))
    val producer = new FlinkKafkaProducer010[ComputeResult](
      args(4),
      new ComputeResultSerializeSchema(args(4)),
      producerProps
    )
    /* at_ least_once 设置 */
    producer.setLogFailuresOnly(false)
    producer.setFlushOnCheckpoint(true)

    /* confStream **/
    val confStream = env.addSource(new ConfSource(args(5)))
      .setParallelism(1)
      .broadcast


    env.addSource(consumer)
      .connect(confStream)
      // ConnectedStreams[LogEvent, ComputeConf]
      .flatMap(new ApplyComputeRule)
      // DataStream[ComputeResult]
      .assignTimestampsAndWatermarks(new BoundedLatenessWatermarkAssigner(args(6).toInt))
      // DataStream[ComputeResult]
      .keyBy(FIELD_KEY)
      // KeyedStream[ComputeResult, Tuple]
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      // WindowedStream[ComputeResult, Tuple, TimeWindow]
      .reduce(_ + _)
      // DataStream[ComputeResult]
      .keyBy(FIELD_KEY, FIELD_PERIODS)
      // KeyedStream[ComputeResult, Tuple]
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(1)))
      // WindowedStream[ComputeResult, Tuple, TimeWindow]
      .apply(new AggregateFunc())
      // DataStream[ComputeResult]
      .addSink(producer)

    env.execute("log_compute")


  }

}