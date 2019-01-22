package com.myFlink.java.batch;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 参考：
 *  https://mp.weixin.qq.com/s?__biz=MjM5MDY1NjgxNA==&mid=2247483720&idx=1&sn=8509c804fa528d59c24dd0f5591a11ee&chksm=a64031099137b81f3d1f7926d967b21b144567f5e46687e385220c74c9cd6bf7ec54c089e864&mpshare=1&scene=23&srcid=1231yRhMRVPRGClmuTIGYQ7X#rd
 *  实时sql topN
 *  https://yq.aliyun.com/articles/457445
 */
public class TopN {

    public static void main(String[] args) throws Exception {
        TextOutputFormat outputFormat = new TextOutputFormat(new Path("F:\\gitworkspace\\myFlink\\result.txt"));
        outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //==================================================================================================

        env.fromElements(
                new Tuple2<>("x", 1),
                new Tuple2<>("y", 3),
                new Tuple2<>("z", 2),
                new Tuple2<>("x", 3),
                new Tuple2<>("y", 3),
                new Tuple2<>("z", 3)
        ).groupBy(0).sum(1).sortPartition(1, Order.DESCENDING).setParallelism(1).first(2).output(outputFormat);

        //===============================================结合 sortGroup 来实现===========================================

        DataSource<Tuple2<String, Integer>> dataSource = env.fromElements(
                new Tuple2<>("x", 4),
                new Tuple2<>("y", 3),
                new Tuple2<>("z", 2),
                new Tuple2<>("x", 3),
                new Tuple2<>("y", 3),
                new Tuple2<>("z", 3)
        ).setParallelism(2);

        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> firstTopN = dataSource
                .groupBy(0).sum(1)
                .groupBy(0).sortGroup(1, Order.DESCENDING)
                .first(2);

        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> allTopN = firstTopN
                .groupBy(0).sum(1)
                .sortPartition(1, Order.DESCENDING).setParallelism(1).first(2);
        allTopN.output(outputFormat);

        System.out.println(env.getExecutionPlan());

        // =========================================结合 mapPartition 来实现=========================================
        // 消耗的内存会更小

        DataSource<Tuple2<String, Integer>> dataSource1 = env.fromElements(
                new Tuple2<>("x", 1),
                new Tuple2<>("y", 3),
                new Tuple2<>("z", 12),
                new Tuple2<>("x", 3),
                new Tuple2<>("y", 3),
                new Tuple2<>("z", 3)
        ).setParallelism(2);

        MapPartitionFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> mapPartitionFunction =
                new MapPartitionFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private Integer minNum = 0;
                    private Integer maxNum = 0;
                    private List<Tuple2<String, Integer>> topN = new ArrayList<>();

                    @Override
                    public void mapPartition(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out)
                            throws Exception {
                        values.forEach(obj -> {
                            if (obj.f1 >= maxNum) {
                                topN.add(0, obj);
                            } else if (obj.f1 > minNum) {
                                topN.set(topN.size() - 1, obj);
                            }
                            if (topN.size() > 12) {
                                topN.remove(topN.size() - 1);
                            }
                        });
                        topN.forEach(a -> {
                            out.collect(a);
                        });
                    }
                };

        MapPartitionOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> firstTopN1 = dataSource1
                .groupBy(0).sum(1)
                .mapPartition(mapPartitionFunction);

        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> allTopN1 = firstTopN1
                .groupBy(0).sum(1)
                .sortPartition(1, Order.DESCENDING).setParallelism(1).first(2);
        allTopN1.output(outputFormat);


        env.execute("top N demo");
    }
}
