package org.infinivision.flink.streaming.process;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Types;
import org.apache.flink.util.Collector;
import org.infinivision.flink.streaming.sources.FinSources;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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

//        wcount.print();

        DataStream<Tuple2<Long, Integer>> result = wcount
                .keyBy(0)
                .process(new TopNItems(3));

        result.print();

        env.execute("TopN Example");

    }

    static final class TopNItems extends KeyedProcessFunction<Tuple, Tuple2<Long, Integer>, Tuple2<Long, Integer>> {
        private int topSize;

        // 存储收集到的商品，待收齐完同一个窗口的数据后，再触发计算
        private ListState<Tuple2<Long, Integer>> itemState;

        public TopNItems(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<Tuple2<Long, Integer>> itemStateDesc = new ListStateDescriptor<>(
                    "item-states",
                    TypeInformation.of(new TypeHint<Tuple2<Long, Integer>>() {
                    })
            );

            itemState = getRuntimeContext().getListState(itemStateDesc);
        }

        @Override
        public void processElement(
                Tuple2<Long, Integer> value,
                Context ctx,
                Collector<Tuple2<Long, Integer>> out) throws Exception {

            itemState.add(value);

            // get the timestamp of the record
            Long timestamp = ctx.timestamp();
            ctx.timerService().registerEventTimeTimer(timestamp);

        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<Long, Integer>> out) throws Exception {

            List<Tuple2<Long, Integer>> items = new ArrayList<>();
            for (Tuple2<Long, Integer> item : itemState.get()) {
                items.add(item);
            }

            itemState.clear();

            // sort the items
            items.sort(new Comparator<Tuple2<Long, Integer>>() {
                @Override
                public int compare(Tuple2<Long, Integer> o1, Tuple2<Long, Integer> o2) {
                    return o2.f1 - o1.f1;
                }
            });

            for(int i=0; i<items.size() && i<topSize; i++) {
                out.collect(items.get(i));
            }
        }
    }
}
