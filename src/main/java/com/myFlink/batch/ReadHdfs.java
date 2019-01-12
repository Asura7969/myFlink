package com.myFlink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * TODO - 未完
 */
public class ReadHdfs {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> source = env.readTextFile("hdfs://xxx:9000/linkLog/partitonTime=2019-01-03_10/part-00000-7107261e-a513-4692-a323-6cc677ab3ef5.c000.json");


        source.print();

        env.execute("ReadHdfs");
    }
}
