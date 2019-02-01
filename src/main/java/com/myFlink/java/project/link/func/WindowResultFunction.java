package com.myFlink.java.project.link.func;

import com.myFlink.java.project.link.bean.Node;
import com.myFlink.java.project.link.bean.SoaLog;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class WindowResultFunction implements WindowFunction<List<SoaLog>,TreeMap<String,Node>,Tuple,TimeWindow> {

    private static final Logger LOGGER = LoggerFactory.getLogger("WindowResultFunction");
    private static final String REQUEST_CLIENT = "REQUEST_CLIENT";
    private static final String REQUEST_SERVER = "REQUEST_SERVER";

    @Override
    public void apply(Tuple key, // 窗口的主键，即 reqId
                      TimeWindow window,
                      Iterable<List<SoaLog>> input,
                      Collector<TreeMap<String,Node>> out) throws Exception {

        List<SoaLog> soa = input.iterator().next();
        Map<String, List<SoaLog>> collect = soa.stream().collect(Collectors.groupingBy(SoaLog::getRpcId));
        TreeMap<String,Node> link = new TreeMap<>();
        collect.keySet().forEach(k -> {
            List<SoaLog> soaLogs = collect.get(k);
            if (soaLogs.size() == 1) {
                LOGGER.error("size:1,reqId:{}",soaLogs.get(0).getReqId());
            } else if (soaLogs.size() == 2){
                aggrNodeMsg(link,k,soaLogs);
            } else {
                List<SoaLog> logs = soaLogs.stream().distinct().collect(Collectors.toList());
                aggrNodeMsg(link,k,logs);
            }
        });

        out.collect(link);
    }

    public static void aggrNodeMsg(TreeMap<String,Node> link,String rpcId, List<SoaLog> logs){
        Map<String, List<SoaLog>> col = logs.stream().collect(Collectors.groupingBy(SoaLog::getMetric));
        List<SoaLog> client = col.get(REQUEST_CLIENT);
        List<SoaLog> server = col.get(REQUEST_SERVER);

        client.forEach(c_soa -> {
            String c_iFace = c_soa.getiFace();
            String c_service = c_soa.getService();
            String c_method = c_soa.getMethod();

            server.forEach(s_soa -> {
                if (c_iFace.equals(s_soa.getiFace()) &&
                    c_service.equals(s_soa.getService()) &&
                    c_method.equals(s_soa.getMethod())) {
                    link.put(rpcId,new Node(rpcId,s_soa.getAppId(),c_soa.getAppId(),c_iFace,c_service,c_method,c_soa.getIpAddress()));
                }
            });
        });
    }
}