package org.infinivision.flink.streaming.tablesql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TableDistinctTest {
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

        StreamQueryConfig qConfig = tableEnv.queryConfig();
        qConfig.withIdleStateRetentionTime(Time.minutes(1), Time.minutes(6));

        DataStream<Tuple3<Integer, Integer, Long>> input = steamEnv.socketTextStream(hostname, port, "\n")
                .flatMap(new Tokenizer());

        Table adInfo = tableEnv.fromDataStream(input, "aid, count, ts");

        adInfo.printSchema();

        Table adInfoDistinct = adInfo.distinct();
        // append stream
        // retract stream
        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(adInfoDistinct, Row.class);
        result.print();
        steamEnv.execute("stream distinct test");

    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple3<Integer, Integer, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple3<Integer, Integer, Long>> out) {
            // normalize and split the line
            String[] wordCount = value.toLowerCase().trim().split(",\\s*");

            int aid = Integer.parseInt(wordCount[0]);
            int count = Integer.parseInt(wordCount[1]);
            long ts = Long.parseLong(wordCount[2]);
            System.out.println("aid: " + aid + " count: " + count + " ts: " + ts);
            out.collect(new Tuple3<>(aid, count, ts));
        }
    }
}
