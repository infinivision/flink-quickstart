package org.infinivision.flink.examples.stream.process;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;


public class KeyWordCount {

    public static void main(String[] args) throws Exception {
        // set up the execution environment

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.get("hostname", "172.19.0.108") ;
            port = params.getInt("port", 9999);

            // make parameters available in the web interface

            env.getConfig().setGlobalJobParameters(params);

        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }


        env.setParallelism(1);



//        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        System.out.println("autowatermark interval: " + env.getConfig().getAutoWatermarkInterval());

        env.getConfig().setAutoWatermarkInterval(30000);
        System.out.println("autowatermark interval: " + env.getConfig().getAutoWatermarkInterval());

        // enable checkpoint
//        env.enableCheckpointing(5000);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");


        DataStream<Tuple2<String, Integer>> stream = text
                .flatMap(new Tokenizer());

        // apply the process function onto a keyed stream
        DataStream<Tuple2<String, Long>> result = stream
                .assignTimestampsAndWatermarks(new AssignTimeStampAndWaterMark())
                .keyBy(f -> f.f0)
                .process(new CountWithTimeoutFunction());

        result.print();

        env.execute("Process Function KeyWordCount");
    }

    public static class AssignTimeStampAndWaterMark implements AssignerWithPeriodicWatermarks<Tuple2<String, Integer>> {

        // 10 seconds lateness
        private long currentWaterMark = 10000l;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            currentWaterMark += 200L;
            return new Watermark(currentWaterMark);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Integer> element, long previousElementTimestamp) {
            System.out.println("previous Element Timestamp: " + previousElementTimestamp);
            return 0L;
        }
    }
    /**
     * The data type stored in the state
     */
    public static class CountWithTimestamp {
        public String key;
        public long count;
        public long lastModified;
    }

    /**
     * The implementation of the ProcessFunction that maintains the count and timeouts
     */
    public static final class CountWithTimeoutFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Long>> {

        /**
         * The state that is maintained by this process function
         */
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }


        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {

            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // update the state's count
            current.count++;

            // set the state's timestamp to the record's assigned event time timestamp
            current.lastModified = ctx.timestamp();

            // write the state back
            state.update(current);

            System.out.println("process Element key: " + ctx.getCurrentKey() +
                    " timestamp: " + ctx.timestamp() + " current watermark: " + ctx.timerService().currentWatermark());

            long current_process_time = ctx.timerService().currentProcessingTime();
            System.out.println("current processing time: " + current_process_time);
            System.out.println("register ProcessingTime timer");
            // register process time
            ctx.timerService().registerProcessingTimeTimer(current_process_time + 5000);
            // schedule the next timer 60 seconds from the current event time
            System.out.println("register EventTime timer");

            ctx.timerService().registerEventTimeTimer(current.lastModified + 1000);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {

            // just get the water periodly
            System.out.println("===Timer Triggered===");

            TimeDomain domain = ctx.timeDomain();
            if(domain == TimeDomain.EVENT_TIME) {
                System.out.println("Event Time Domain");
            }

            if (domain == TimeDomain.PROCESSING_TIME) {
                System.out.println("Processing Time Domain");
            }

            System.out.println("timestamp: " + ctx.timestamp());
            System.out.println("watermark: " + ctx.timerService().currentWatermark());
//            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 1000);

//            // get the state for the key that scheduled the timer
//            CountWithTimestamp result = state.value();
//            System.out.println("===on Timer context===");
//            System.out.println("timestamp: " + ctx.timestamp());
//            System.out.println("watermark: " + ctx.timerService().currentWatermark());
//            // check if this is an outdated timer or the latest timer
//            if (timestamp == result.lastModified + 60000) {
//                // emit the state on timeout
//                out.collect(new Tuple2<>(result.key, result.count));
//            }
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
