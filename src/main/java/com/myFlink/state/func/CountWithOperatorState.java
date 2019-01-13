package com.myFlink.state.func;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 背景：
 *      有一业务场景，数据流事件是:1,2,4,5,7,8,1,3,4,5,1,6,90,1,...
 * 需求：
 *      需要统计每隔事件"1"之间出现多少个事件,分别是哪些事件?
 * 结果：
 *      5 ->  2,4,5,7,8
 *      3 ->  3,4,5
 *      2 ->  6,90
 *      ...
 */
public class CountWithOperatorState extends RichFlatMapFunction<Long,String> implements CheckpointedFunction {

    /** 存储结果状态 */
    private transient ListState<Long> checkPointCountList;
    private List<Long> listBufferElements;

    @Override
    public void flatMap(Long r, Collector<String> collector) throws Exception {
        if (r == 1) {
            if (listBufferElements.size() > 0) {
                StringBuffer buffer = new StringBuffer();
                for (int i = 0; i < listBufferElements.size(); i++) {
                    buffer.append(listBufferElements.get(i) + " ");
                }
                collector.collect(buffer.toString());
                listBufferElements.clear();
            }
        } else {
            listBufferElements.add(r);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkPointCountList.clear();
        for (int i = 0; i < listBufferElements.size(); i++) {
            checkPointCountList.add(listBufferElements.get(i));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor =
                new ListStateDescriptor<Long>(
                        "listForThree",
                        TypeInformation.of(new TypeHint<Long>(){})
                );
        checkPointCountList = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
        if (functionInitializationContext.isRestored()) {
            for (Long elem : checkPointCountList.get()) {
                listBufferElements.add(elem);
            }
        }
    }
}
