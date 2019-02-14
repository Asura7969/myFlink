package com.myFlink.java.project.link;

import com.myFlink.java.project.link.bean.Link;
import com.myFlink.java.project.link.bean.Mark;
import com.myFlink.java.project.link.bean.Node;
import com.myFlink.java.project.link.bean.SoaLog;
import com.myFlink.java.project.link.func.AggrMsgByReqId;
import com.myFlink.java.project.link.func.LinkDetailsFunction;
import com.myFlink.java.project.link.func.WindowResultFunction;
import com.myFlink.java.project.link.utils.TransFormSoaLog;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.*;

public class LinkStream {

    private static final String REQUEST_CLIENT = "REQUEST_CLIENT";
    private static final String REQUEST_SERVER = "REQUEST_SERVER";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE)
                .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ExecutionConfig config = env.getConfig();
        //config.setAutoWatermarkInterval(1000L);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(8000L);
        // 只允许2个检查点同时进行
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);

        //env.setStateBackend(new RocksDBStateBackend("hdfs_path",true));

        Properties kafkaConf = new Properties();
        kafkaConf.setProperty("bootstrap.servers", "kafka1:9092");
        kafkaConf.setProperty("group.id", "flink-group");

        FlinkKafkaConsumer010<String> consumer =
                new FlinkKafkaConsumer010<>("soa-info", new SimpleStringSchema(), kafkaConf);

        // 一个TreeMap一条链路,有重复数据
        env.addSource(consumer)
                .map((MapFunction<String, SoaLog>) log -> TransFormSoaLog.lookUp(log))
                .filter(soa -> filterMsg(soa))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SoaLog>(Time.seconds(30)) {
                    @Override
                    public long extractTimestamp(SoaLog element) {
                        return element.getLogTime();
                    }
                })
                //.assignTimestampsAndWatermarks(new BoundedLatenessWatermarkAssigner())
                .keyBy("reqId")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //.allowedLateness(Time.seconds(60))
                .aggregate(new AggrMsgByReqId(), new WindowResultFunction())
                .keyBy("windowEnd")
                .process(new LinkDetailsFunction())
                .print();

        env.execute("link Stream");

    }

    public static boolean filterMsg(SoaLog soaLog){
        boolean flag = soaLog.getMetric().equals(REQUEST_CLIENT) || soaLog.getMetric().equals(REQUEST_SERVER);
        return flag && soaLog.getMark() == Mark.USEFUL;
    }

    public static class BoundedLatenessWatermarkAssigner implements AssignerWithPeriodicWatermarks<SoaLog> {

        private long maxTimestamp = -1L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp - 20 * 1000L);
        }

        @Override
        public long extractTimestamp(SoaLog element, long previousElementTimestamp) {
            long logTime = element.getLogTime();
            if (logTime > maxTimestamp) {
                maxTimestamp = logTime;
            }
            return logTime;
        }
    }

}
