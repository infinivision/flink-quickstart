/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.infinivision.flink.streaming.states;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.infinivision.flink.streaming.wordcount.util.WordCountData;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 */
public class WordCountState {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        String host = params.get("hostname", "localhost");
        int port = params.getInt("port", 9999);


        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration configuration = new Configuration();
//        configuration.setString("akka.ask.timeout", "10 h");
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.getConfig().setGlobalJobParameters(params);


        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(1000); // create a checkpoint every 5 seconds
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // make parameters available in the web interface

        env.setParallelism(1);
        // get input data
        DataStream<String> text = env.socketTextStream(host, port, "\n");


        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0)
                        .map(new CountFunction());

        // emit result
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        // execute program
        env.execute("Streaming WordCount States");
    }


    public static class CountFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>
            implements CheckpointedFunction {

        private ReducingState<Long> countPerKey;
        private ListState<Long> countPerPartition;

        private long localCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("CountFunction initialize state");
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
            System.out.println("CountFunction snapshot state");
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
