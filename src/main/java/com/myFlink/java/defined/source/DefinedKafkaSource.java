package com.myFlink.java.defined.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class DefinedKafkaSource implements SourceFunction<String> {

    private static final long serialVersionUID = 1L;
    private volatile boolean isRunning = true;
    private volatile int i = 0;
    private int counter = 0;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            if(counter % 5 == 0){
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service4\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_CLIENT\"}}");
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service5\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_SERVER\"}}");
            } else if (counter % 4 == 0) {
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service4\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_CLIENT\"}}");
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service5\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_SERVER\"}}");
            } else if (counter % 3 == 0) {
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service4\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_CLIENT\"}}");
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service5\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_SERVER\"}}");
            } else if (counter % 2 == 0) {
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service4\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_CLIENT\"}}");
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service5\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_SERVER\"}}");
            } else {
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service4\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_CLIENT\"}}");
                ctx.collect("ANALYSIS,2019-02-19 10:31:33.793,2,{\"condition\":{\"appid\":\"service5\",\"iface\":\"com.easybike.dictionary.service.DictionaryService\",\"method\":\"getCityCodeByAdcode\",\"peerIp\":{\"address\":\"10.111.61.239\",\"port\":48100},\"service\":\"AppHellobikeDictionaryService\",\"rpcid\":\"1.1.1\",\"reqid\":\"123456\"},\"entityMata\":{\"ipAddress\":\"10.111.63.75\",\"metric\":\"RESPONSE_SERVER\"}}");
            }

            counter++;
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
