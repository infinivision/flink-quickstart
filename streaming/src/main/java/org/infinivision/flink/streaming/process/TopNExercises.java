package org.infinivision.flink.streaming.process;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TopNExercises {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setLong("heartbeat.interval", 1000000);
        conf.setLong("heartbeat.timeout", 5000000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);

        final ParameterTool params = ParameterTool.fromArgs(args);
        final String hostname = params.get("hostname", "localhost") ;
        final int port = params.getInt("port", 9999);

        DataStream<Tuple2<String, Integer>> wcount = env
                .socketTextStream(hostname, port)
                .flatMap(new Tokenizer())
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .sum(1);

//        wcount.print();

        DataStream<Tuple2<String, Integer>> result = wcount
                .timeWindowAll(Time.seconds(20))
                .max(1);

        result.print();

        env.execute("TopN Example");

    }

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
