package com.myFlink.java.cep.login;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * https://blog.csdn.net/dounine/article/details/84207797
 */
public class LoginFail {

    public static final String FAIL = "fail";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟事件流
        DataStream<LoginEvent> loginEventStream = env.fromCollection(Arrays.asList(
                new LoginEvent("1","192.168.0.1","fail"),
                new LoginEvent("1","192.168.0.1","fail"),
                new LoginEvent("1","192.168.0.1","fail"),
                new LoginEvent("2","192.168.10,10","success")
        ));

        // 定义规则,在1秒时间内都是登录失败
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>
                begin("begin")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return FAIL.equals(loginEvent.getType());
                    }
                })
                .next("next")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return FAIL.equals(loginEvent.getType());
                    }
                })
                .within(Time.seconds(1));


        PatternStream<LoginEvent> patternStream =
                CEP.pattern(
                        loginEventStream.keyBy(LoginEvent::getUserId), loginFailPattern
                );

        DataStream<LoginWarning> loginFailDataStream =
                patternStream.select((Map<String, List<LoginEvent>> pattern) -> {
                    List<LoginEvent> first = pattern.get("begin");
                    List<LoginEvent> second = pattern.get("next");
                    return new LoginWarning(second.get(0).getUserId(),second.get(0).getIp(), second.get(0).getType());
                });

        loginFailDataStream.print();

        env.execute();

    }
}
