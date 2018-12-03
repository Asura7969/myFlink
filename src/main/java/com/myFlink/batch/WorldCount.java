package com.myFlink.batch;

import com.myFlink.utils.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WorldCount {

    private static final String INPUT_KEY = "input";
    private static final String OUTPUT_KEY = "output";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text;
        if(params.has(INPUT_KEY)){
            text = env.readTextFile(params.get(INPUT_KEY));
        } else {
            text = WordCountData.getDefaultTextLineDataSet(env);
        }

        DataSet<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer()).groupBy(0)
          .sum(1);

        if (params.has(OUTPUT_KEY)) {
            counts.writeAsCsv(params.get(OUTPUT_KEY), "\n", " ");
            // execute program
            env.execute("WordCount Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }
    }

    private static final class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            Arrays.stream(tokens).forEach(token -> {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            });
        }
    }
}
