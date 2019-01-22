package com.myFlink.java.project.ConsumerKafka;

import com.myFlink.java.project.opentsdb.WriteIntoOpentsdb;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class StreamKafka {

    public static void main(String[] args) throws Exception {

        final String KAFKA_HOST_PORT = "master1:9092,master2:9092,slave1:9092";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE)
                .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /** 默认的全局配置: flink-conf.yaml --->  state.backend */
        env.setStateBackend(new MemoryStateBackend(2048));

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_HOST_PORT);
        props.setProperty("group.id", "transaction");

        FlinkKafkaConsumer010<String> consumer =
                new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), props);

        DataStreamSource<String> transction = env.addSource(consumer);

        transction.rebalance()
                .map(value -> {
                    String[] result = value.split(" ");
                    WriteIntoOpentsdb write = new WriteIntoOpentsdb();
                    write.writeIntoOpentsdb(result);
                    return value;
                })
                .print();

        env.execute("StreamKafka");


    }
}
