package com.myFlink.java.project.link.func;

import com.myFlink.java.project.link.bean.Link;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcess extends KeyedProcessFunction<Tuple,Link,Link>{
    @Override
    public void processElement(Link value, Context ctx, Collector<Link> out) throws Exception {

    }
}
