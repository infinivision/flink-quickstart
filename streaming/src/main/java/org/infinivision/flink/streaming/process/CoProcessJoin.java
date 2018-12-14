package org.infinivision.flink.streaming.process;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.infinivision.flink.streaming.entity.AdFeature;

import java.util.Iterator;


public class CoProcessJoin {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        String inputPath = params.get("input", "/root/flink_jobs/adFeature.csv");

        PojoTypeInfo<AdFeature> typeInfo = (PojoTypeInfo<AdFeature>)TypeExtractor.createTypeInfo(AdFeature.class);
        PojoCsvInputFormat<AdFeature> inputFormat = new PojoCsvInputFormat<>(new Path(inputPath), typeInfo, new String[]{
                "aid", "advertiserId" , "campaignId", "creativeId", "creativeSize", "adCategoryId", "productId", "productType"
        });

        inputFormat.setSkipFirstLineAsHeader(true);
        DataStream<AdFeature> adStream1 = env.createInput(inputFormat, typeInfo)
                .process(new LoadAllAdFeature())
                .keyBy("aid");

        DataStream<AdFeature> adStream2 = env.createInput(inputFormat, typeInfo)
                .keyBy("aid");

        DataStream<Tuple2<String, String>> output = adStream1.connect(adStream2)
                .process(new ProcessAdFeatureId());

        output.print();
        env.execute("coprocess join");
    }

    public final static class LoadAllAdFeature extends ProcessFunction<AdFeature, AdFeature> {

        @Override
        public void open(Configuration parameters) throws Exception {
        }

        @Override
        public void processElement(AdFeature value, Context ctx, Collector<AdFeature> out) {

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdFeature> out) throws Exception {

        }
    }


    public static class ProcessAdFeatureId extends CoProcessFunction<AdFeature, AdFeature, Tuple2<String, String>> {
        // keyed, managed state
        private ListState<AdFeature> adState1;
        private ListState<AdFeature> adState2;

        @Override
        public void open(Configuration config) throws Exception {
            adState1 = getRuntimeContext().getListState(new ListStateDescriptor<>("saved adfeature1", AdFeature.class));
            adState2 = getRuntimeContext().getListState(new ListStateDescriptor<>("saved adfeature2", AdFeature.class));
        }

        @Override
        public void processElement1(AdFeature value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception{
            System.out.println("processElement1 AdFeature: " + value.toString());
            adState1.add(value);
            Iterator<AdFeature> state2It = adState2.get().iterator();

            // join state1 with state2
            if (state2It.hasNext()) {
                for (AdFeature state1: adState1.get()) {
                    for ( AdFeature state2 : adState2.get()) {
                        System.out.println("Element1 Join with state2: " + state2.toString());
                        out.collect(new Tuple2<>(state1.getAid(), state2.getAdvertiserId()));
                        out.collect(new Tuple2<>(state1.getAid() + "ProcessElement1", state2.getAdvertiserId()));

                    }
                }
            } else {
                System.out.println("ProcessElement1 no state2 found");
            }


        }

        @Override
        public void processElement2(AdFeature value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception{
            System.out.println("processElement2 AdFeature: " + value.toString());
            adState2.add(value);

            Iterator<AdFeature> state1It = adState1.get().iterator();
            if (state1It.hasNext()) {
                // join state1 with state2
                for (AdFeature state1: adState1.get()) {
                    for ( AdFeature state2 : adState2.get()) {
                        System.out.println("processElement2 Join with state2: " + state2.toString());
                        out.collect(new Tuple2<>(state1.getAid(), state2.getAdvertiserId()));
                        out.collect(new Tuple2<>(state1.getAid() + "ProcessElement2", state2.getAdvertiserId()));
                    }
                }

                // clear state2
                adState2.clear();
            } else {
                System.out.println("ProcessElement2 no state1 found");
            }

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

            System.out.println("process Element key: " + ctx.getCurrentKey());

            // schedule the next timer 60 seconds from the current event time
            ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();

            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 60000) {
                // emit the state on timeout
                out.collect(new Tuple2<>(result.key, result.count));
            }
        }
    }


}
