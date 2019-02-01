package com.myFlink.java.project.link.func;

import com.myFlink.java.project.link.bean.SoaLog;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

public class AggrMsgByReqId implements AggregateFunction<SoaLog,List<SoaLog>,List<SoaLog>> {

    @Override
    public List<SoaLog> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<SoaLog> add(SoaLog value, List<SoaLog> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public List<SoaLog> getResult(List<SoaLog> accumulator) {
        //accumulator.sort((o1, o2) -> {
        //    if (o1.getLogTime() == o2.getLogTime()) {
        //        return 0;
        //    } else {
        //        return o1.getLogTime() < o2.getLogTime() ? 1 : -1;
        //    }
        //});
        return accumulator;
    }

    @Override
    public List<SoaLog> merge(List<SoaLog> a, List<SoaLog> b) {
        a.addAll(b);
        return a;
    }
}
