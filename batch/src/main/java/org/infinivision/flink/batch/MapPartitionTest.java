package org.infinivision.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.infinivision.flink.batch.util.WordCountData;

public class MapPartitionTest {

    public static class PartitionCounter implements MapPartitionFunction<String, Integer> {
        @Override
        public void mapPartition(Iterable<String> values, Collector<Integer> out) {
            System.out.println("====Enter Map Partition====");

            int count = 0;
            for(String s : values) {
                System.out.println("str: " + s);
                count++;
            }

            out.collect(count);
        }
    }

    public static class IncreaseWordIndex implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static int index = 1;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, index++));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(2);
//        DataSet<String> input = WordCountData.getDefaultTextLineDataSet(env);
        DataSet<String> input = WordCountData.getDefaultTextLineDataSet(env);


        System.out.println("parallelism: " + env.getParallelism());
        DataSet<Integer> output = input.mapPartition(new PartitionCounter());
        output.print();


//        DataSet<Tuple2<String, Integer>> output = input.flatMap(new IncreaseWordIndex());
//        output.print();
    }


}
