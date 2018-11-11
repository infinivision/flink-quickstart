package org.infinivision.flink.batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.infinivision.flink.batch.util.KaggleData;

import java.lang.reflect.Method;
import java.util.Iterator;

public class GroupReduceTest {

    // GroupReduceFunction that removes consecutive identical elements
    public static class DistinctGroupReduce
            implements GroupReduceFunction<Tuple2<Integer, String>, String> {

        @Override
        public void reduce(Iterable<Tuple2<Integer, String>> in, Collector<String> out) {
            Integer key = null;
            String comp = null;

            System.out.println("===Enter Reduce Function===");
            for (Tuple2<Integer, String> t : in) {
                key = t.f0;
                String next = t.f1;

                System.out.println("key: " + key + " value: " + next);
                // check if strings are different
                if ( !next.equals(comp)) {
//                    out.collect(new Tuple2<Integer, String>(key, next));
                    out.collect(next);

                    comp = next;
                }
            }
        }
    }

    // count the number of grouped fields
    public static class CountGroupReduce
            implements GroupReduceFunction<Tuple2<Integer, String>, Tuple2<String, Integer>> {

        private String prefix;

        public CountGroupReduce() {
        }

        public CountGroupReduce(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public void reduce(Iterable<Tuple2<Integer, String>> in, Collector<Tuple2<String, Integer>> out) {
            String key = null;
            Integer num = 0;

            Iterator<Tuple2<Integer, String>> it = in.iterator();
            System.out.println("===Enter Count Reduce Function===");
            while (it.hasNext()) {
                key = it.next().f1;
                num++;
            }

            if (key != null) {
                out.collect(new Tuple2<>(prefix+key, num));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<Tuple2<Integer, String>> input = env.fromElements(
                new Tuple2<>(3, "c"),
                new Tuple2<>(3, "b"),
                new Tuple2<>(3, "b"),
                new Tuple2<>(5, "a"),
                new Tuple2<>(5, "d"),
                new Tuple2<>(7, "f")
                );

//        DataSet<String> output = input.groupBy(0)
//                .sortGroup(1, Order.ASCENDING)
//                .reduceGroup(new DistinctGroupReduce());
//
//        output.print();

        // count the grouped fields
        DataSet<Tuple2<String, Integer>> countGroup = input.groupBy(1)
                .reduceGroup(new CountGroupReduce("Hello"));

        countGroup.print();


//        env.execute("GroupReduce Example");
    }
}
