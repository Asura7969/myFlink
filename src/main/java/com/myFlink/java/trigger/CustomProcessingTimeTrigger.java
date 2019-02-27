package com.myFlink.java.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TriggerResult:
 * <ul>
 *     <li>CONTINUE: 什么也不做</li>
 *     <li>FIRE: 触发计算</li>
 *     <li>PURGE: 清除窗口中的数据</li>
 *     <li>FIRE_AND_PURGE: 触发计算并清除窗口中的数据</li>
 * </ul>
 * https://mp.weixin.qq.com/s/8wxiC1QLpCHvnWjTJGOhXw
 */
public class CustomProcessingTimeTrigger extends Trigger<Object,TimeWindow>{

    private static final long serialVersionUID = -639708801485741882L;

    private CustomProcessingTimeTrigger() {}

    private static int flag = 0;

    /**
     * 每个元素被添加到窗口时调用
     */
    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        // CONTINUE 是代表不做输出，也即是，此时我们想要实现比如10条输出一次，
        // 而不是窗口结束再输出就可以在这里实现。
        if(flag > 9){
            flag = 0;
            return TriggerResult.FIRE;
        }else{
            flag ++;
        }
        System.out.println("onElement : "+element);
        return TriggerResult.CONTINUE;
    }

    /**
     * 当一个已注册的处理时间计时器启动时调用
     */
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    /**
     * 当一个已注册的事件时间计时器启动时调用
     */
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    /**
     * 执行任何需要清除的相应窗口
     */
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    /**
     * 与状态性触发器相关，当使用会话窗口时，两个触发器对应的窗口合并时，合并两个触发器的状态。
     */
    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "ProcessingTimeTrigger()";
    }

    public static CustomProcessingTimeTrigger create() {
        return new CustomProcessingTimeTrigger();
    }
}
