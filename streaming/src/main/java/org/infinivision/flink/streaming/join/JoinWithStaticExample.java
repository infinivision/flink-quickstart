package org.infinivision.flink.streaming.join;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class JoinWithStaticExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        System.out.println("checkpoint interval: " + env.getCheckpointInterval());
        // a streaming source that keeps running indefinitely
        DataStream<Long> dynamicSource = env.addSource(new SourceFunction<Long>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                Random rand = new Random();

                int count = 0;
                while (isRunning && count <= 10) {
                    Thread.sleep(1000);
                    ctx.collect((long) rand.nextInt(3));
                    count++;
                }

                System.out.println("Dynamic Source Current System Time: " + System.currentTimeMillis());

            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        // a finite source that eventually stops, this will emit a Watermark(Long.MAX_VALUE) when
        // finishing
        DataStream<Tuple2<Long, String>> staticSource = env.addSource(new SourceFunction<Tuple2<Long, String>>() {
            private volatile boolean isRunning = true;
            @Override
            public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
                // delay a bit so that the join operator actually has to buffer elements from
                // the first input
                Thread.sleep(5000);
                ctx.collect(new Tuple2<>(0L, "a"));
                ctx.collect(new Tuple2<>(1L, "b"));
                ctx.collect(new Tuple2<>(2L, "c"));
                System.out.println("Static Source Current System Time: " + System.currentTimeMillis());
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        KeyedStream<Long, Long> keyedDynamic = dynamicSource.keyBy(new KeySelector<Long, Long>() {
            @Override
            public Long getKey(Long value) throws Exception {
                return value;
            }
        });

        KeyedStream<Tuple2<Long, String>, Long> keyedStatic = staticSource.keyBy(new KeySelector<Tuple2<Long,String>, Long>() {
            @Override
            public Long getKey(Tuple2<Long, String> value) throws Exception {
                return value.f0;
            }
        });

        keyedDynamic.connect(keyedStatic)
                .transform("custom join",
                        new TypeHint<Tuple3<Long, Long, Tuple2<Long, String>>>() {}.getTypeInfo(),
                        new JoinOperator<Long, Long, Tuple2<Long, String>>(BasicTypeInfo.LONG_TYPE_INFO, new TypeHint<Tuple2<Long, String>>() {}.getTypeInfo()))
                .print();

        // execute program
        env.execute("Join Example");
    }


    public static class JoinOperator<K, I1, I2>
            extends AbstractStreamOperator<Tuple3<K, I1, I2>>
            implements TwoInputStreamOperator<I1, I2, Tuple3<K, I1, I2>> {

        private boolean waitingForStaticInput;

        private ListStateDescriptor<I1> dynamicListStateDescriptor;
        private ListStateDescriptor<I2> staticListStateDescriptor;

        private ListState<I2> staticBufferedElements;
        private ListState<I1> dynamicBufferedElements;

        // this part is a bit of a hack, we manually keep track of the keys for which we
        // have buffered elements. This can change once the state allows iterating over all keys
        // we need this to iterate over the buffered input elements once we receive the watermark
        // from the second input
        private Set<K> inputKeys;


        public JoinOperator(TypeInformation<I1> dynamicType, TypeInformation<I2> staticType) {
//            dynamicListStateDescriptor = new ListStateDescriptor<>("dynamic-elements", dynamicType);
//            staticListStateDescriptor = new ListStateDescriptor<>("static-elements", staticType);
//            staticBufferedElements = getRuntimeContext().getListState(staticListStateDescriptor);
//            dynamicBufferedElements = getRuntimeContext().getListState(dynamicListStateDescriptor);
        }

        @Override
        public void open() throws Exception {
            super.open();
//            waitingForStaticInput = true;
//            inputKeys = new HashSet<>();
        }


        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void processElement1(StreamRecord<I1> element) throws Exception {
            System.out.println("Dynamic Input Record: " + element.toString());
            System.out.println("Current Key: " + getCurrentKey());

        }

        @Override
        public void processElement2(StreamRecord<I2> element) throws Exception {
            System.out.println("Static Input Record: " + element.toString());
            System.out.println("CurrentKey: " + getCurrentKey());

        }

        @Override
        public void processWatermark1(Watermark mark) throws Exception {
            System.out.println("Dynamic Input WaterMark: " + mark.getTimestamp());
            System.out.println("Static Source Current System Time: " + System.currentTimeMillis());


        }
            // we are not interrested in those

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void processWatermark2(Watermark mark) throws Exception {
            System.out.println("Static Input WaterMark: " + mark.getTimestamp());
            System.out.println("Static Source Current System Time: " + System.currentTimeMillis());

        }
//            if (mark.getTimestamp() == Long.MAX_VALUE) {
//                waitingForStaticInput = false;
//
//                // perform nested loop join for the buffered elements from input 1
//                for (K key: inputKeys) {
//                    setCurrentKey(key);
//                    for (I1 storedElement: dynamicBufferedElements.get()) {
//                        // the elements we get here are scoped to the same key as the input element
//                        for (I2 joinElement : staticBufferedElements.get()) {
//                            System.out.println("JOINING FOR STORED ELEMENT: " + joinElement);
//                            output.collect(new StreamRecord<>(new Tuple3<>((K)getCurrentKey(),
//                                    storedElement,
//                                    joinElement)));
//                        }
//                    }
//
//                    // clean out the stored elements
//                    dynamicBufferedElements.clear();
//                }
//                inputKeys = null;
//            }
//        }
    }

    /**
     * Assume that the second input is the static input. We wait on a Long.MAX_VALUE watermark
     * from the second input and buffer the elements from the first input until that happens. Then
     * we just continue streaming by elements from the first input.
     *
     * <p>This assumes that both inputs are keyed on the same key K.
     */

    /**
     *
     * @param <K>
     * @param <I1>
     * @param <I2>
    public static class JoinOperator<K, I1, I2>
            extends AbstractStreamOperator<Tuple3<K, I1, I2>>
            implements TwoInputStreamOperator<I1, I2, Tuple3<K, I1, I2>> {

        private boolean waitingForStaticInput;

        private ListStateDescriptor<I1> dynamicListStateDescriptor;
        private ListStateDescriptor<I2> staticListStateDescriptor;

        private ListState<I2> staticBufferedElements;
        private ListState<I1> dynamicBufferedElements;

        // this part is a bit of a hack, we manually keep track of the keys for which we
        // have buffered elements. This can change once the state allows iterating over all keys
        // we need this to iterate over the buffered input elements once we receive the watermark
        // from the second input
        private Set<K> inputKeys;


        public JoinOperator(TypeInformation<I1> dynamicType, TypeInformation<I2> staticType) {
            dynamicListStateDescriptor = new ListStateDescriptor<>("dynamic-elements", dynamicType);
            staticListStateDescriptor = new ListStateDescriptor<>("static-elements", staticType);
            staticBufferedElements = getRuntimeContext().getListState(staticListStateDescriptor);
            dynamicBufferedElements = getRuntimeContext().getListState(dynamicListStateDescriptor);
        }

        @Override
        public void open() throws Exception {
            super.open();
            waitingForStaticInput = true;
            inputKeys = new HashSet<>();
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void processElement1(StreamRecord<I1> element) throws Exception {

            if (waitingForStaticInput) {
                // store the element for when the static input is available
                dynamicBufferedElements.add(element.getValue());
                inputKeys.add((K)getCurrentKey());
                System.out.println("STORING INPUT ELEMENT FOR LATER: " + element.getValue());
            } else {
                // perform nested-loop join

                // the elements we get here are scoped to the same key as the input element
                for (I2 joinElement : staticBufferedElements.get()) {
                    output.collect(new StreamRecord<>(new Tuple3<>((K)getCurrentKey(), element.getValue(), joinElement)));
                }
            }
        }

        @Override
        public void processElement2(StreamRecord<I2> element) throws Exception {
            // store for joining with elements from primary input
            staticBufferedElements.add(element.getValue());
        }

        @Override
        public void processWatermark1(Watermark mark) throws Exception {
            // we are not interrested in those
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void processWatermark2(Watermark mark) throws Exception {
            if (mark.getTimestamp() == Long.MAX_VALUE) {
                waitingForStaticInput = false;

                // perform nested loop join for the buffered elements from input 1
                for (K key: inputKeys) {
                    setCurrentKey(key);
                    for (I1 storedElement: dynamicBufferedElements.get()) {
                        // the elements we get here are scoped to the same key as the input element
                        for (I2 joinElement : staticBufferedElements.get()) {
                            System.out.println("JOINING FOR STORED ELEMENT: " + joinElement);
                            output.collect(new StreamRecord<>(new Tuple3<>((K)getCurrentKey(),
                                    storedElement,
                                    joinElement)));
                        }
                    }

                    // clean out the stored elements
                    dynamicBufferedElements.clear();
                }
                inputKeys = null;
            }
        }
    }
     */

}