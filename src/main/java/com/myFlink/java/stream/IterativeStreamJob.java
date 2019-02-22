package com.myFlink.java.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 迭代运算
public class IterativeStreamJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        IterativeStream<Long> itStream = env.generateSequence(0, 100).iterate();

        // 基于输入流构建迭代头
        DataStream<Long> minusOne = itStream.map(value -> value - 1L);
        // 定义迭代逻辑(map,fun等)
        DataStream<Long> genaterThanZero = minusOne.filter(value -> value > 0L);

        // 方法可以关闭一个迭代（也可以表述为定义了迭代尾）
        itStream.closeWith(genaterThanZero);

        // 定义终止的迭代逻辑（符合条件的元素将被分发给下游而不用于进行下一次迭代）,并打印
        minusOne.filter(value -> value <= 0L).print();

        env.execute("IterativeStreamJob");

    }

}
