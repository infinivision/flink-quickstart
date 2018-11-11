package org.infinivision.flink.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class TableJoinTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        BatchTableEnvironment bTableEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<Tuple3<String, String, String>> ds1 = env.fromElements(
                new Tuple3<>("AA", "123", "sh"),
                new Tuple3<>("BB", "456", "sd"),
                new Tuple3<>("CC", "789", "jn")
        );

        Table left =  bTableEnv.fromDataSet(ds1, "a, b, c");


        DataSet<Tuple3<String, String, String>> ds2 = env.fromElements(
                new Tuple3<>("AA", "123", "sh"),
                new Tuple3<>("DD", "456", "sd"),
                new Tuple3<>("FF", "789", "jn"),
                new Tuple3<>("EE", "789", "jn")
        );

        if (bTableEnv.config().getNullCheck()) {
            System.out.println("Table Null Check Enabled");
        }

        Table right =  bTableEnv.fromDataSet(ds2, "d, e, f");

        Table result = left.leftOuterJoin(right, "a=d")
                .select("a, b, e")
                .filter("e.isNotNull");

        TupleTypeInfo<Tuple3<String, String, String>> tupleTypeInfo = new TupleTypeInfo<>(
                Types.STRING(),
                Types.STRING(),
                Types.STRING()
        );

        System.out.println("===Table API LeftOuterJoin===");
        bTableEnv.toDataSet(result, tupleTypeInfo).print();

        System.out.println("===DataSet LeftOuterJoin===");
        ds1.leftOuterJoin(ds2)
                .where(0)
                .equalTo(0)
                .with(new LeftOuterJoinOp())
                .print();
    }


    public static class LeftOuterJoinOp implements JoinFunction<Tuple3<String, String, String>,
            Tuple3<String, String, String>,
            Tuple3<String, String, String>> {

        @Override
        public Tuple3<String, String, String> join(Tuple3<String, String, String> left,
                                                   Tuple3<String, String, String> right) {
            if (right == null) {
                System.out.println("right is null. left.f0: " + left.f0);
                return new Tuple3<>(left.f0, left.f1, "000");
            } else {
                return new Tuple3<>(left.f0, left.f1, right.f1);
            }

        }
    }
}
