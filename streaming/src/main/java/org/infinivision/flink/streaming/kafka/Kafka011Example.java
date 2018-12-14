
package org.infinivision.flink.streaming.kafka;

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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.infinivision.flink.streaming.entity.TrainEvent;
import org.infinivision.flink.streaming.entity.TrainEventSchema;

import javax.annotation.Nullable;

/**
 * A simple example that shows how to read from and write to Kafka. This will read String messages
 * from the input topic, parse them into a POJO type {@link TrainEvent}, group by some key, and finally
 * perform a rolling addition on each key for which the results are written back to another topic.
 *
 * <p>This example also demonstrates using a watermark assigner to generate per-partition
 * watermarks directly in the Flink Kafka consumer. For demonstration purposes, it is assumed that
 * the String messages are of formatted as a (word,frequency,timestamp) tuple.
 *
 * <p>Example usage:
 * 	flink run -c org.infinivision.flink.examples.stream.kafka.Kafka011Example --input-topic tecent-train --output-topic tecent-train-count --bootstrap.servers 172.19.0.108:9092 --group.id myconsumer
 */
public class Kafka011Example {

	public static void main(String[] args) throws Exception {
		// parse input arguments
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 4) {
			System.out.println("Missing parameters!\n" +
					"Usage: Kafka --input-topic <topic> --output-topic <topic> " +
					"--bootstrap.servers <kafka brokers> " +
					"--group.id <some id>");
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(60000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// set the parallelism to 3 for multiple partitions read
		env.setParallelism(3);

		// set the state backend per job
//        env.setStateBackend(new FsStateBackend("s3://flink/flink-checkpoints"));

		DataStream<TrainEvent> input = env
				.addSource(
					new FlinkKafkaConsumer011<>(
						parameterTool.getRequired("input-topic"),
						new TrainEventSchema(),
						parameterTool.getProperties())
					.assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
				.keyBy("uid")
				.map(new RollingAdditionMapper());

		input.addSink(
				new FlinkKafkaProducer011<>(
						parameterTool.getRequired("output-topic"),
						new TrainEventSchema(),
						parameterTool.getProperties()));

		env.execute("Kafka 0.11 Example");
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
