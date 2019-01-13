package com.myFlink.state;

import com.myFlink.state.func.CountWithKeyedState;
import com.myFlink.state.func.CountWithOperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class KeyedStateMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.fromElements(
                Tuple2.of(1L,3L),
                Tuple2.of(10L,5L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,3L),
                Tuple2.of(3L,9L),
                Tuple2.of(1L,3L),
                Tuple2.of(2L,7L),
                Tuple2.of(6L,8L))
                .keyBy(0)
                .flatMap(new CountWithKeyedState())
                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {

                    }
                });

        env.execute("KeyedStateMain");
    }
}
