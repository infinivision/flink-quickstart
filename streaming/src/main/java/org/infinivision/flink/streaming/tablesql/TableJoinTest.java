package org.infinivision.flink.streaming.tablesql;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TableJoinTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // the host and the port to connect to
        final String hostname = params.get("hostname", "localhost");

        final int port = params.getInt("port", 9999);


        // set up the execution environment
        final StreamExecutionEnvironment steamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        steamEnv.getConfig().setGlobalJobParameters(params);
        steamEnv.getConfig().setAutoWatermarkInterval(1000);
        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(steamEnv);
        // make parameters available in the web interface

        DataStream<Tuple3<Integer, Integer, Long>> input = steamEnv.socketTextStream(hostname, port, "\n")
                .flatMap(new Tokenizer());

        Table adInfo = tableEnv.fromDataStream(input, "aid, count, ts");

        adInfo.printSchema();

        tableEnv.connect(
                new Kafka()
                        .version("0.11")
                        .topic("tecent-train")
                        .property("group.id", "kafka-consumer")
                        .property("bootstrap.servers", "172.19.0.108:9092")
                        .startFromEarliest()

        )
                .withFormat(
                        new Json()
                                .deriveSchema()
                )
//        .withFormat(
//                new Json()
//                .jsonSchema(
//                        "{" +
//                        "  type: 'object'," +
//                        "  properties: {" +
//                        "    aid: {" +
//                        "      type: 'number'" +
//                        "    }," +
//                        "    uid: {" +
//                        "      type: 'number'" +
//                        "    }," +
//                        "    label: {" +
//                        "      type: 'number'" +
//                        "    }" +
//                        "  }" +
//                        "}"
//                )
//        )
                .withSchema(
                        new Schema()
                                .field("aid", Types.INT)
                                .field("uid", Types.INT)
                                .field("label", Types.INT)
                )
                .inAppendMode()
                .registerTableSource("kafka-tecent-train");


        Table tecentTrain = tableEnv.scan("kafka-tecent-train")
                .select("aid as aaid, uid, label");

        tecentTrain.printSchema();

        Table result = adInfo.join(tecentTrain, "aid == aaid");
        result.printSchema();
        // append stream
        DataStream<Row> ds = tableEnv.toAppendStream(result, Row.class);

        ds.print();
        // retract stream
//        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(tecentTrain, Row.class);
//        result.print();
        steamEnv.execute("stream join test");

    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple3<Integer, Integer, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple3<Integer, Integer, Long>> out) {
            // normalize and split the line
            String[] wordCount = value.toLowerCase().trim().split(",\\s*");

            out.collect(new Tuple3<>(Integer.parseInt(wordCount[0]), Integer.parseInt(wordCount[1]), Long.getLong(wordCount[2])));
        }
    }
}
