package com.myFlink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class WorldCountSql {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<Wc> input = env.fromElements(
                new Wc("Spark", 1),
                new Wc("Flink", 1),
                new Wc("Storm", 1)
        );

        tEnv.registerDataSet("WordCount", input, "word, frequency");
        Table table = tEnv.sqlQuery("SELECT word ,SUM(frequency) AS frequency FROM WordCount GROUP BY word");

        DataSet<Wc> result = tEnv.toDataSet(table, Wc.class);

        result.print();

    }

    public static class Wc{
        public String word;
        public long frequency;

        public Wc() {
        }

        public Wc(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "Wc{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }
}
