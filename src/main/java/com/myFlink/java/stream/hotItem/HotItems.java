package com.myFlink.java.stream.hotItem;

import com.myFlink.java.stream.hotItem.pojo.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * https://mp.weixin.qq.com/s/tzyacswXcxgwHS1YwenPkQ
 */
public class HotItems {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");

        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType =(PojoTypeInfo<UserBehavior>)TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

        //创建输入源
        DataStreamSource<UserBehavior> dataSource  = env.createInput(csvInput, pojoType);
        /**
         * ProcessingTime：事件被处理的时间。也就是由机器的系统时间来决定
         * EventTime：事件发生的时间。一般就是数据本身携带的时间
         * 以上两个选择具体看业务
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        /**
         * 如果事件是乱序的：
         *      BoundedOutOfOrdernessTimestampExtractor
         * 该实例中时间是单调递增的，选择：
         *      AscendingTimestampExtractor
         */
        DataStream<UserBehavior> timedData = dataSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>(){
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        // 原始数据单位秒，将其转成毫秒
                        return userBehavior.timestamp * 1000;
                    }
                });


        DataStream<UserBehavior> pvData = timedData
                .filter(userBehavior -> {
                    // 过滤出只有点击的数据
                    return userBehavior.behavior.equals("pv");
                });

        /**
         * 需求：
         *      每隔5分钟统计一次最近一小时每个商品的点击量
         */
        DataStream<ItemViewCount> windowedData = pvData
                .keyBy("itemId")
                .timeWindow(Time.minutes(60),Time.minutes(5))
                /**
                 * 增量的聚合操作,它能使用AggregateFunction提前聚合掉数据,减少 state 的存储压力
                 *
                 * 第一个参数：CountAgg
                 *      统计窗口中的条数，遇到一条数据就加一
                 *
                 * 第二个参数：WindowResultFunction
                 *      将每个 key每个窗口聚合后的结果带上其他信息进行输出
                 */
                .aggregate(new CountAgg(),new WindowResultFunction());

        /**
         * TopN 计算最热门商品
         */
        DataStream<String> topItems = windowedData
                .keyBy("windowEnd")
                .process(new TopNHotItems(3));  // 求点击量前3名的商品


        topItems.print();
        env.execute("Hot Items Job");
    }

    /**
     * COUNT 统计的聚合函数实现，每出现一条记录加一
     */
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple key,// 窗口的主键，即 itemId
                          TimeWindow window,// 窗口
                          Iterable<Long> aggregateResult,// 聚合函数的结果，即 count 值
                          Collector<ItemViewCount> collector // 输出类型为 ItemViewCount
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    public static class ItemViewCount {
        public long itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<ItemViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(
                ItemViewCount input,
                Context context,
                Collector<String> collector) throws Exception {

            // 每条数据都保存到状态中
            itemState.add(input);
            // 注册 windowEnd + 1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            context.timerService().registerEventTimeTimer(input.windowEnd + 1);
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort((o1, o2) -> (int) (o2.viewCount - o1.viewCount));
            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < topSize; i++) {
                ItemViewCount currentItem = allItems.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("====================================\n\n");

            out.collect(result.toString());
        }
    }
}
