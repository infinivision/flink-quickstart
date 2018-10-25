package org.infinivision.flink.examples.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class CoGroupTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<Tuple2<Integer, Integer>> dataSet = env.fromElements(
                new Tuple2<Integer, Integer>(1, 2),
                new Tuple2<Integer, Integer>(3, 4),
                new Tuple2<Integer, Integer>(5, 6),
                new Tuple2<Integer, Integer>(7, 8)
        );


    }
}
