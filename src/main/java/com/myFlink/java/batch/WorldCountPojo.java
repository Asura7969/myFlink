package com.myFlink.java.batch;

import com.myFlink.java.utils.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WorldCountPojo {
    private static final String INPUT_KEY = "input";
    private static final String OUTPUT_KEY = "output";

    public static class Word {

        private String word;
        private int frequency;

        public Word() {}

        public Word(String word, int i) {
            this.word = word;
            this.frequency = i;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "Word : " + word + " freq : " + frequency;
        }
    }


    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text;
        if (params.has(INPUT_KEY)) {
            text = env.readTextFile(params.get(INPUT_KEY));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = WordCountData.getDefaultTextLineDataSet(env);
        }

        DataSet<Word> counts =
                text.flatMap(new Tokenizer())
                        .groupBy("word")
                        .reduce(new MyReduce());

        if (params.has(OUTPUT_KEY)) {
            counts.writeAsText(params.get(OUTPUT_KEY), FileSystem.WriteMode.OVERWRITE);
            env.execute("WordCount-Pojo Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }
    }

    public static final class Tokenizer implements FlatMapFunction<String, Word> {

        @Override
        public void flatMap(String value, Collector<Word> out) {
            String[] tokens = value.toLowerCase().split("\\W+");

            Arrays.stream(tokens).forEach(token -> {
                if (token.length() > 0) {
                    out.collect(new Word(token, 1));
                }
            });
        }
    }

    public static final class MyReduce implements ReduceFunction<Word>{

        @Override
        public Word reduce(Word value1, Word value2) throws Exception {
            return new Word(value1.word, value1.frequency + value2.frequency);
        }
    }

}
