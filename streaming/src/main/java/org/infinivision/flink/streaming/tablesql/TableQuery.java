package org.infinivision.flink.streaming.tablesql;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TableQuery {
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

//        DataStream<Tuple3<String, Integer, Long>> input = steamEnv.socketTextStream(hostname, port, "\n")
//                .flatMap(new Tokenizer());

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
                .field("aid", "DECIMAL")
                .field("uid", "DECIMAL")
                .field("label", "DECIMAL")
        )
        .inAppendMode()
        .registerTableSource("kafka-tecent-train");


        Table tecentTrain = tableEnv.scan("kafka-tecent-train")
                .select("aid, uid, label")
                // sort on stream tables is currently not support
                .orderBy("aid");
//                .groupBy("aid")
//                .select("aid, max(uid)");
        tecentTrain.printSchema();

        RelNode relNode = tecentTrain.getRelNode();
        System.out.println("Convention Name: " + relNode.getConvention().getName());
        System.out.println("CorrelVariable: " + relNode.getCorrelVariable());
        System.out.println("RelTypeName: " + relNode.getRelTypeName());

        // append stream
//        DataStream<Row> result = tableEnv.toAppendStream(tecentTrain, Row.class);

        // retract stream
        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(tecentTrain, Row.class);
        result.print();
        steamEnv.execute("kafka sql query");

    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple3<String, Integer, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple3<String, Integer, Long>> out) {
            // normalize and split the line
            String[] wordCount = value.toLowerCase().trim().split(",\\s*");

            out.collect(new Tuple3<>(wordCount[0], Integer.parseInt(wordCount[1]), Long.getLong(wordCount[2])));
        }
    }
}
