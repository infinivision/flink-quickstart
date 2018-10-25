package org.infinivision.flink;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.util.Collector;
import org.infinivision.flink.util.KaggleData;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class KaggleDataLGBEncode {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // create a TableEnvironment
        BatchTableEnvironment bTableEnv = TableEnvironment.getTableEnvironment(env);

        String inPath = params.get("input", "/root/flink_jobs/train.txt");
        String outPath = params.get("output", "/root/flink_jobs/lgb_train.csv");
        Long cutOff = params.getLong("cutoff", 0L);

        CsvTableSource csvTableSource = CsvTableSource.builder()
                .path(inPath)
                .field("label", Types.INT())
                .field("i1", Types.STRING())
                .field("i2", Types.STRING())
                .field("i3", Types.STRING())
                .field("i4", Types.STRING())
                .field("i5", Types.STRING())
                .field("i6", Types.STRING())
                .field("i7", Types.STRING())
                .field("i8", Types.STRING())
                .field("i9", Types.STRING())
                .field("i10", Types.STRING())
                .field("i11", Types.STRING())
                .field("i12", Types.STRING())
                .field("i13", Types.STRING())
                .field("c1", Types.STRING())
                .field("c2", Types.STRING())
                .field("c3", Types.STRING())
                .field("c4", Types.STRING())
                .field("c5", Types.STRING())
                .field("c6", Types.STRING())
                .field("c7", Types.STRING())
                .field("c8", Types.STRING())
                .field("c9", Types.STRING())
                .field("c10", Types.STRING())
                .field("c11", Types.STRING())
                .field("c12", Types.STRING())
                .field("c13", Types.STRING())
                .field("c14", Types.STRING())
                .field("c15", Types.STRING())
                .field("c16", Types.STRING())
                .field("c17", Types.STRING())
                .field("c18", Types.STRING())
                .field("c19", Types.STRING())
                .field("c20", Types.STRING())
                .field("c21", Types.STRING())
                .field("c22", Types.STRING())
                .field("c23", Types.STRING())
                .field("c24", Types.STRING())
                .field("c25", Types.STRING())
                .field("c26", Types.STRING())
                .fieldDelimiter("\t")
                .lineDelimiter("\n")
                .build();

        // register table
        bTableEnv.registerTableSource("train", csvTableSource);

        // scan table
        Table train = bTableEnv.scan("train");

        env.setParallelism(8);

        DataSet<KaggleData> lgbDs = bTableEnv.toDataSet(train, KaggleData.class);

        // handle continuous features

        TupleTypeInfo<Tuple3<Integer, Integer, Integer>> intIntTypeInfo = new TupleTypeInfo<>(
                Types.INT(),
                Types.INT(),
                Types.INT()
        );

        DataSet<Tuple3<Integer, Integer, Integer>> dummpy = env.fromElements(
                new Tuple3<>(0, Integer.MIN_VALUE, Integer.MAX_VALUE)
        );

        Table minMax = bTableEnv.fromDataSet(dummpy, "id, min, max");
        for (int fieldId=1; fieldId<=13; fieldId++) {
            Table tmp = train.select(String.format("i%d", fieldId))
                    .filter(String.format("i%d != ''", fieldId))
                    .select(String.format("i%d.cast(INT) as i%d", fieldId, fieldId))
                    .select(String.format("%d as id, i%d.min as min, i%d.max as max", fieldId, fieldId, fieldId));

            minMax = minMax.union(tmp);
        }


        DataSet<Tuple3<Integer, Integer, Integer>> minMaxDs = bTableEnv.toDataSet(minMax, intIntTypeInfo)
                .map(new NormalizeMaxFeature())
                .sortPartition(0, Order.ASCENDING).setParallelism(1);

        lgbDs = lgbDs.map(new MapFeatureValue())
             .withBroadcastSet(minMaxDs, "minMax");


        // handle category feature
        TupleTypeInfo<Tuple2<String, Long>> tupleTypeInfo = new TupleTypeInfo<>(
                Types.STRING(),
                Types.LONG()
        );

        for (int fieldId=1; fieldId<=26; fieldId++) {
            String selectFields = String.format("c%d", fieldId);
            String selectExpression = String.format("c%d, c%d.count", fieldId, fieldId);
            String filterExpression = String.format("c%d != ''", fieldId);

            Table c1 = train.select(selectFields)
                    .groupBy(selectFields)
                    .select(selectExpression)
                    .filter(filterExpression);

            DataSet<Tuple2<String, Integer>> ds = bTableEnv.toDataSet(c1, tupleTypeInfo)
                    .filter(new FilterIndex(cutOff))
                    .sortPartition(1, Order.DESCENDING).setParallelism(1)
                    .sortPartition(0, Order.ASCENDING)
                    .map(new MapFeatureIndex());

//            String groupedOutPath = String.format("/root/flink_jobs/c%dgrouped_lgb.csv", fieldId);
//            ds.writeAsCsv(groupedOutPath, WriteMode.OVERWRITE);

            // LeftOuterJoin trainds with c1Ds
            lgbDs = lgbDs.leftOuterJoin(ds, JoinHint.BROADCAST_HASH_SECOND)
                    .where(selectFields)
                    .equalTo(0)
                    .with(new LeftOuterJoinOp(fieldId));
        }

        // write to csv
        lgbDs.writeAsText(outPath, WriteMode.OVERWRITE).setParallelism(1);
        env.execute("Kaggle Data LGB Encode");
    }


    public static class MapFeatureValue extends RichMapFunction<KaggleData, KaggleData> {

        List<Tuple3<Integer, Integer, Integer>> minMaxes;
        @Override
        public void open(Configuration parameters) throws Exception {
            minMaxes = getRuntimeContext().getBroadcastVariable("minMax");
        }

        @Override
        public KaggleData map(KaggleData value) {
            for(int index=1; index<=13; index++) {
                int min = minMaxes.get(index).f1;
                int max = minMaxes.get(index).f2;
                String v = value.getI(index);
                if(v.length() == 0) {
                    value.setI(index, "0");
                } else {
                    float result = (float)(Integer.valueOf(v)-min)/(max-min);
                    value.setI(index, String.format("%.4f", result));
                }
            }
            return value;
        }
    }

    public static class NormalizeMaxFeature
            implements MapFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {

        private final int[] thresholds = {0, 20, 600, 100, 50, 64000, 500, 100, 50, 500, 10, 10, 10, 50};

        @Override
        public Tuple3<Integer, Integer, Integer> map(Tuple3<Integer, Integer, Integer> input) {
            int fieldId = input.f0;
            int min = input.f1;
            int max = input.f2;
            if (max > thresholds[fieldId]) {
                max = thresholds[fieldId];
            }

            return new Tuple3<>(fieldId, min, max);
        }
    }

    public static class MapFeatureIndex implements MapFunction<Tuple2<String, Long>, Tuple2<String, Integer>> {
        private Integer index = 1;

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Long> value) {
            return new Tuple2<>(value.f0, index++);
        }
    }

    public static class FilterIndex implements FilterFunction<Tuple2<String, Long>> {
        private Long cutOff = 100L;

        public FilterIndex(Long cutOff) {
            this.cutOff = cutOff;
        }

        @Override
        public boolean filter(Tuple2<String, Long> value) {
            return value.f1 >= cutOff;
        }
    }

    public static class LeftOuterJoinOp implements JoinFunction<KaggleData, Tuple2<String, Integer>, KaggleData> {

        private int fieldIndex;

        public LeftOuterJoinOp(int fieldIndex) {
            this.fieldIndex = fieldIndex;
        }

        @Override
        public KaggleData join(KaggleData left, Tuple2<String, Integer> right) {
            if (right == null) {
                left.setC(fieldIndex, "0");
            } else {
                left.setC(fieldIndex, Integer.toString(right.f1));
            }
            return left;
        }
    }
}
