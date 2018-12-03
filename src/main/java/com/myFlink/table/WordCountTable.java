package com.myFlink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class WordCountTable {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<WC> input = env.fromElements(
                new WC("Spark", 1),
                new WC("Flink", 1),
                new WC("Storm", 1)
        );

        Table table = tEnv.fromDataSet(input);
        Table filtered = table.groupBy("word")
                .select("word, frequency.sum as frequency")
                .where("frequency = 1");

        DataSet<WC> result = tEnv.toDataSet(filtered,WC.class);

        //toString 方法 决定打印的输出格式
        result.print();
    }


    public static class WC{
        public String word;
        public long frequency;

        public WC() {
        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "Wc " + word + "  frequency " + frequency;
        }
    }
}
