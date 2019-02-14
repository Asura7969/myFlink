package com.myFlink.java.project.link.func;

import com.myFlink.java.project.link.bean.Link;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LinkDetailsFunction extends KeyedProcessFunction<Tuple, Link, String> {

    private ListState<Link> linkState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ListStateDescriptor<Link> itemsStateDesc =
                new ListStateDescriptor<>("link-state", Link.class);
        linkState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void processElement(Link input, Context ctx, Collector<String> out) throws Exception {
        linkState.add(input);
        ctx.timerService().registerEventTimeTimer(input.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        Map<String,Integer> allLinks = new HashMap<>();
        for (Link link : linkState.get()) {
            Integer count = allLinks.getOrDefault(link.toString(), 0);
            allLinks.put(link.toString(),count + 1);
        }
        linkState.clear();

        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
        allLinks.forEach((l,c) -> result.append(l).append(",").append(c).append("\n"));
        out.collect(result.toString());
    }
}
