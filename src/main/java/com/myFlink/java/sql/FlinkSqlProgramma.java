package com.myFlink.java.sql;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * flink sql 编程示例
 */
public class FlinkSqlProgramma {

    private final static String KAFKA_HOST_PORT = "master1:9092,master2:9092,slave1:9092";

    public static void main(String[] args) throws Exception {

        // 自定义函数使用
        Configuration conf = new Configuration();
        conf.setString("hashcode_factor","31");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(conf);

        final StreamTableEnvironment tbEnv = TableEnvironment.getTableEnvironment(env);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", KAFKA_HOST_PORT);
        kafkaProperties.setProperty("group.id", "group");

        // 定义与注册输入表
        tbEnv.connect(new Kafka().version("0.10")
                .topic("input").properties(kafkaProperties).startFromEarliest())
//                .withFormat(new Avro().recordClass(SdkLog.class))
//                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
                // 标记是否在缺少字段时失败,默认 false
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(new Schema()
                        .field("id", Types.BOOLEAN)
                        .field("name",Types.STRING)
                        .field("age",Types.INT)
                        .field("",Types.SQL_TIMESTAMP)
                            .proctime()
                        .field("",Types.SQL_TIMESTAMP)
                            .rowtime(new Rowtime()
                                    .timestampsFromField("logTime")))
                .inAppendMode()
                .registerTableSource("srcTable");
        // 定义与注册输出表
        tbEnv.connect(new Kafka().version("0.10")
                .topic("output").properties(kafkaProperties).startFromEarliest())
//                .withFormat(new Avro().recordClass(SdkLog.class))
//                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(new Schema()
                        .field("id", Types.BOOLEAN)
                        .field("name",Types.STRING)
                        .field("age",Types.INT))
                .inAppendMode()
                .registerTableSource("dstTable");
        // 注册UDF
        tbEnv.registerFunction("doubleFunc", new DoubleToInt());

        // 转 Table API
//        Table srcTable = tbEnv.scan("srcTable");
//        final Table counts = srcTable.groupBy("age")
//                .select("name.count as cnt");

        // 执行SQL
        tbEnv.sqlUpdate("INSERT INTO dstTable SELECT id, name, doubleFunc(age) " +
                "FROM srcTable WHERE id = 1004");

        env.execute("flink sql demo");

    }

    /**
     *  http://zhuanlan.51cto.com/art/201811/586881.htm
     *  参考: https://cloud.tencent.com/developer/article/1172926
      * ScalarFunction:
      *      A user-defined scalar functions maps zero, one, or multiple scalar values to a new scalar value.
      *      实现 public ... eval(...)
      *
      * TableFunction:
      *      (UDTF) A user-defined table functions works on zero, one, or multiple scalar values as input and
      *      returns multiple rows as output.
      * Example:
      * {{{
      *
      *   public class Split extends TableFunction<String> {
      *
      *     // implement an "eval" method with as many parameters as you want
      *     public void eval(String str) {
      *       for (String s : str.split(" ")) {
      *         collect(s);   // use collect(...) to emit an output row
      *       }
      *     }
      *
      *     // you can overload the eval method here ...
      *   }
      *
      *   val tEnv: TableEnvironment = ...
      *   val table: Table = ...    // schema: [a: String]
      *
      *   // for Scala users
      *   val split = new Split()
      *   table.join(split('c) as ('s)).select('a, 's)
      *
      *   // for Java users
      *   tEnv.registerFunction("split", new Split())   // register table function first
      *   table.join(new Table(tEnv, "split(a) as (s)")).select("a, s")
      *
      *   // for SQL users
      *   tEnv.registerFunction("split", new Split())   // register table function first
      *   tEnv.sqlQuery("SELECT a, s FROM MyTable, LATERAL TABLE(split(a)) as T(s)")
      *
      * }}}
      * AggregateFunction:
      *
      */
    private static class DoubleToInt extends ScalarFunction{
        private int factor = 0;
        @Override
        public void open(FunctionContext context) throws Exception {
            // 获取全局参数
            factor = Integer.valueOf(context.getJobParameter("hashcode_factor","12"));
        }

        public double eval(int a) {
            return Double.valueOf(String.valueOf(a)) * factor;
        }
    }
    public static class TimestampModifier extends ScalarFunction {
        public long eval(long t) {
            return t % 1000;
        }
        // 将long型的返回值在代码生成时翻译成
        @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.SQL_TIMESTAMP;
        }
    }

    private static class Split extends TableFunction<String> {
        private String separator = " ";
        public Split(String separator) {
            this.separator = separator;
        }
        public void eval(String s) {
            for (String v : s.split(separator)) {
                collect(v);
            }
        }
    }

    public class CustomTypeSplit extends TableFunction<Row> {
        public void eval(String str) {
            for (String s : str.split(" ")) {
                Row row = new Row(2);
                row.setField(0, s);
                row.setField(1, s.length());
                collect(row);
            }
        }
        // 定义复杂的返回类型
        @Override
        public TypeInformation<Row> getResultType() {
            return Types.ROW(Types.STRING, Types.INT);
        }
    }
}
