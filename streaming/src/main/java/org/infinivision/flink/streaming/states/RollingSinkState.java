package org.infinivision.flink.streaming.states;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.infinivision.flink.streaming.entity.TrainEvent;
import org.infinivision.flink.streaming.entity.TrainEventSchema;
import org.infinivision.flink.streaming.kafka.Kafka011Example;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.Properties;

public class RollingSinkState {
    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        InputStream inputStream = RollingSinkState.class.getClassLoader().getResourceAsStream("kafka.properties");
        Properties properties = new Properties();
        properties.load(inputStream);

//        Configuration configuration = new Configuration();
//        configuration.setString("akka.ask.timeout", "10 h");
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 1000));
        env.enableCheckpointing(1000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // set the parallelism to 3 for multiple partitions read
        env.setParallelism(1);


        FlinkKafkaConsumer011<TrainEvent> kafkaConsumer011 = new FlinkKafkaConsumer011<>(
                properties.getProperty("input-topic"),
                new TrainEventSchema(),
                properties);

        kafkaConsumer011.setStartFromEarliest();
        kafkaConsumer011.assignTimestampsAndWatermarks(new CustomWatermarkExtractor());
        DataStream<TrainEvent> input = env
                .addSource(kafkaConsumer011)
                .keyBy("uid")
                .map(new RollingAdditionMapper());

        // add the Bucketing Sink
        input.addSink(new BucketingSink<>("/bigdata/flink_data/outputs"));

        env.execute("Rolling Sink Example");
    }

    /**
     * A {@link RichMapFunction} that continuously outputs the current total frequency count of a key.
     * The current total count is keyed state managed by Flink.
     */
    private static class RollingAdditionMapper extends RichMapFunction<TrainEvent, TrainEvent> {

        private static final long serialVersionUID = 1180234853172462378L;

        private transient ValueState<Integer> currentTotalCount;

        @Override
        public TrainEvent map(TrainEvent event) throws Exception {
            Integer totalCount = currentTotalCount.value();

            if (totalCount == null) {
                totalCount = 0;
            }
            totalCount += event.getLabel();

            currentTotalCount.update(totalCount);

            return new TrainEvent(event.getAid(), event.getUid(), totalCount, event.getTimestamp());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            currentTotalCount = getRuntimeContext().getState(new ValueStateDescriptor<>("currentTotalCount", Integer.class));
        }
    }

    /**
     * A custom {@link AssignerWithPeriodicWatermarks}, that simply assumes that the input stream
     * records are strictly ascending.
     *
     * <p>Flink also ships some built-in convenience assigners, such as the
     * {@link BoundedOutOfOrdernessTimestampExtractor} and {@link AscendingTimestampExtractor}
     */
    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<TrainEvent> {

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(TrainEvent event, long previousElementTimestamp) {
            // the inputs are assumed to be of format (message,timestamp)
            this.currentTimestamp = event.getTimestamp();
            return event.getTimestamp();
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}
