package org.infinivision.flink.streaming.local;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.infinivision.flink.streaming.wordcount.util.WordCountData;

public class WordCount {
    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(1);
        // get input data
        DataStream<String> text;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // get default test text data
            text = env.fromElements(WordCountData.WORDS);
        }

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new org.infinivision.flink.streaming.wordcount.WordCount.Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0)
                        .map(new org.infinivision.flink.streaming.wordcount.WordCount.CountFunction());

        // emit result
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        // execute program
        env.execute("Streaming WordCount");
    }


    public static class CountFunction implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>, CheckpointedFunction {

        private ReducingState<Long> countPerKey;
        private ListState<Long> countPerPartition;

        private long localCount;

        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initialize state");
            // get the state data structure for the per-key state
            countPerKey = context.getKeyedStateStore().getReducingState(
                    new ReducingStateDescriptor<>("perKeyCount", (a, b) -> a + b, Long.class));

            // get the state data structure for the per-key state
            countPerPartition = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("perPartitionCount", Long.class));

            // initialize the "local count variable" based on the operator state
            for (Long l : countPerPartition.get()) {
                localCount += l;
            }
        }


        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshot state");
            // the keyed state is always up to date anyways
            // just bring the per-partition state in shape
            countPerPartition.clear();
            countPerPartition.add(localCount);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
            if (countPerPartition == null) {
                System.out.println("count per partition is null");
            }
            countPerKey.add(1L);
            localCount++;
            return new Tuple2<>(value.f0, countPerKey.get().intValue());
        }
    }


    public static class CountPerKeyFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private ValueState<Long> state;

        public void open(Configuration cfg) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myCount", Long.class));
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
            Long current = state.value();
            state.update(current == null ? 1L : current + value.f1);
            return new Tuple2<>(value.f0, state.value().intValue());
        }
    }
// *************************************************************************
// USER FUNCTIONS
// *************************************************************************

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