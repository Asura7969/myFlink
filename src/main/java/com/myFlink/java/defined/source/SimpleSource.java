package com.myFlink.java.defined.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleSource implements SourceFunction<Long>{

    private long count = 0L;
    private volatile boolean isRunning;

    @Override
    public void run(SourceContext<Long> sct) throws Exception {
        while (isRunning && count < 1000) {
            synchronized (sct.getCheckpointLock()) {
                sct.collect(count);
                count ++;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
