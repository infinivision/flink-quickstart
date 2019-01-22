package org.infinivision.flink.streaming.process;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Types;
import org.apache.flink.util.Collector;
import org.infinivision.flink.streaming.sources.FinSources;

public class TopNExercises {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setLong("heartbeat.interval", 1000000);
        conf.setLong("heartbeat.timeout", 5000000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple2<Long, Integer>> wcount = FinSources
                .tradeSource(env)
                .map(t -> Tuple2.of(t.customerId, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, Integer>>() {
                }))
                .keyBy(s -> s.f0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        wcount.print();

//        DataStream<Tuple2<String, Integer>> result = wcount
//                .timeWindowAll(Time.seconds(10))
//                .maxBy(1);


//        result.print();

        env.execute("TopN Example");

    }

    public static class AggPerKey extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>  {
        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple2<String, Integer>> elements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            int sum = 0;
            for(Tuple2<String, Integer> element : elements) {
                sum += element.f1;
            }

            System.out.println("current Key: " + key);
            System.out.println("sum: " + sum);
            out.collect(new Tuple2<>(key, sum));
        }
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
