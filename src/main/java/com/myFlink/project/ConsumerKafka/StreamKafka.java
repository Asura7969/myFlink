package com.myFlink.project.ConsumerKafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class StreamKafka {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE)
                .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /** 默认的全局配置: flink-conf.yaml --->  state.backend */
        env.setStateBackend(new MemoryStateBackend(2048));

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.111.20.155:9092,10.111.20.156:9092,10.111.20.157:9092,10.111.20.229:9092,10.111.20.228:9092,10.111.20.240:9092,10.111.20.241:9092,10.111.20.243:9092,10.111.20.242:9092,10.111.20.239:9092");
        props.setProperty("group.id", "test");

        FlinkKafkaConsumer010<String> consumer =
                new FlinkKafkaConsumer010<>("soa-info", new SimpleStringSchema(), props);

        env.addSource(consumer);
    }
}
