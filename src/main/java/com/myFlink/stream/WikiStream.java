package com.myFlink.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;


/**
 * Wikipedia Edit Stream
 * 参考：https://blog.csdn.net/boling_cavalry/article/details/85205622
 */
public class WikiStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();

        evn.addSource(new WikipediaEditsSource())
                // 以用户名为key分组
                .keyBy((KeySelector<WikipediaEditEvent,String>) WikipediaEditEvent::getUser)
                // 时间窗口为5秒
                .timeWindow(Time.seconds(5))
                // 在时间窗口内按照key将所有数据做聚合
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple3<String,Integer,StringBuilder>, Tuple3<String,Integer,StringBuilder>>() {
                    @Override
                    public Tuple3<String, Integer, StringBuilder> createAccumulator() {
                        return new Tuple3<>("",0,new StringBuilder());
                    }

                    @Override
                    public Tuple3<String, Integer, StringBuilder> add(WikipediaEditEvent wikipediaEditEvent,
                                                                      Tuple3<String, Integer, StringBuilder> tuple3) {
                        StringBuilder sbud = tuple3.f2;
                        // 如果是第一条记录,添加前缀
                        if (StringUtils.isBlank(sbud)) {
                            sbud.append("Details : ");
                        } else {
                            sbud.append(" ");
                        }
                        // 聚合逻辑是将改动的字节数累加
                        return new Tuple3<>(wikipediaEditEvent.getUser(),
                                wikipediaEditEvent.getByteDiff() + tuple3.f1,
                                sbud.append(wikipediaEditEvent.getByteDiff()));
                    }

                    @Override
                    public Tuple3<String, Integer, StringBuilder> getResult(Tuple3<String, Integer, StringBuilder> tuple3) {
                        return tuple3;
                    }

                    @Override
                    public Tuple3<String, Integer, StringBuilder> merge(Tuple3<String, Integer, StringBuilder> tuple3,
                                                                        Tuple3<String, Integer, StringBuilder> acc) {
                        // 合并窗口的场景才会用到
                        return new Tuple3<>(tuple3.f0, tuple3.f1 + acc.f1, tuple3.f2.append(acc.f2));
                    }
                })
                .map((MapFunction<Tuple3<String, Integer, StringBuilder>, String>) Tuple3::toString)
                .print();

        evn.execute("WikiStream");


    }
}
