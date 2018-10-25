package org.infinivision.flink.examples.batch;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class JoinTest {
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

        System.out.println("====DataSet MinBy====");
        dataSet.minBy(0,1).print();

        System.out.println("====DataSet MaxBy====");
        DataSet<Tuple2<Integer, Integer>> result = dataSet.maxBy(0,1);
        result.print();

        System.out.println("====DataSet Min====");
        dataSet.min(1).print();

        System.out.println("====DataSet Max====");
        dataSet.max(1).print();


        DataSet<Integer> leftSide = env.fromElements(1, 2, 3, 4, 5);
        DataSet<Tuple2<Integer, String>> leftSide2 = leftSide.map(
                new MapFunction<Integer, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(Integer integer) throws Exception {
                        return new Tuple2<Integer, String>(integer, "left data");
                    }
                });

        DataSet<Integer> rightSide = env.fromElements(4, 5, 6, 7, 8, 9, 10);
        DataSet<Tuple2<Integer, String>> rightSide2 = rightSide.map(
                new MapFunction<Integer, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(Integer integer) throws Exception {
                        return new Tuple2<Integer, String>(integer, "right data");
                    }
                });

        System.out.println("====Inner Join====");
        leftSide2.join(rightSide2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
                    @Override
                    public Object join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) {
                        return new Tuple2<Integer, String>(first.f0, first.f1 + " " + second.f1);

                    }
                })
                .print();

        System.out.println("====left outer Join====");
        leftSide2.leftOuterJoin(rightSide2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
                    @Override
                    public Object join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) {
                        if (second == null) {
                            System.out.println("right side is null. left f0: " + first.f0 + " f1: " + first.f1);
                            return new Tuple2<Integer, String>(first.f0, first.f1);
                        }
                        return new Tuple2<Integer, String>(first.f0, first.f1 + " " + second.f1);
                    }
                }).print();


        System.out.println("====right outer Join====");
        leftSide2.rightOuterJoin(rightSide2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
                    @Override
                    public Object join(Tuple2<Integer, String> left, Tuple2<Integer, String> right) {
                        if (left == null) {
                            System.out.println("left side is null. right f0: " + right.f0 + " f1: " + right.f1);
                            return new Tuple2<Integer, String>(right.f0, right.f1);
                        }
                        return new Tuple2<Integer, String>(right.f0, left.f1 + " " + right.f1);
                    }
                }).print();

        System.out.println("====full outer Join====");
        leftSide2.fullOuterJoin(rightSide2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
                    @Override
                    public Object join(Tuple2<Integer, String> left, Tuple2<Integer, String> right) {
                        if (left == null) {
                            System.out.println("left side is null. right f0: " + right.f0 + " f1: " + right.f1);
                            return new Tuple2<Integer, String>(right.f0, right.f1);
                        }

                        if (right == null) {
                            System.out.println("right side is null. right f0: " + left.f0 + " f1: " + left.f1);
                            return new Tuple2<Integer, String>(left.f0, left.f1);
                        }

                        return new Tuple2<Integer, String>(right.f0, left.f1 + " " + right.f1);
                    }
                }).print();

    }
}
