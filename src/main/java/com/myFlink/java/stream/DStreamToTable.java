package com.myFlink.java.stream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class DStreamToTable {

    public static void main(String[] args) throws Exception {

        final String KAFKA_HOST_PORT = "47.99.61.133:9092,47.110.148.212:9092,47.110.225.177:9092";
        final String ZK_HOST_PORT = "47.99.61.133:2181,47.110.148.212:2181,47.110.225.177:2181";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        tableEnv.connect(
                new Kafka()
                        .version("0.11")
                        .topic("topicName")
                        .startFromLatest()
                        .property("zookeeper.connect",ZK_HOST_PORT)
                        .property("bootstrap.servers",KAFKA_HOST_PORT)
                        .property("group.id","testGroup"))
                .withFormat(new Json().deriveSchema())
                .withSchema(new Schema()
                        .field("field1","string")
                        .field("field2",""))
                .inAppendMode()
                .registerTableSource("tableName");

        env.execute("example");
    }


}
