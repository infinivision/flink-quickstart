
package org.infinivision.flink.examples.stream.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * <p>This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 12345)
 * using the <i>netcat</i> tool via
 * <pre>
 * nc -l 12345
 * </pre>
 * and run this example with the hostname and the port as arguments.
 */
@SuppressWarnings("serial")
public class SocketWindowWordCount {

	public static void main(String[] args) throws Exception {

		// the host and the port to connect to
		final String hostname;
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			port = params.getInt("port");
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount " +
				"--hostname <hostname> --port <port>', where hostname (localhost by default) " +
				"and port is the address of the text server");
			System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
				"type the input text into the command line");
			return;
		}

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        System.out.println("Default AutoWaterMarkInterval: " + env.getConfig().getAutoWatermarkInterval());

        env.getConfig().setAutoWatermarkInterval(5000);
        System.out.println("Set AutoWaterMarkInterval: " + env.getConfig().getAutoWatermarkInterval());

		// get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts = text

				.flatMap(new Splitter())
                .assignTimestampsAndWatermarks(new AssignTimeStampAndWaterMark())
				.keyBy("word")
				.timeWindow(Time.seconds(20))
//                .apply()
                .sum("count");

//				.reduce(new ReduceFunction<WordWithCount>() {
//					@Override
//					public WordWithCount reduce(WordWithCount a, WordWithCount b) {
//						return new WordWithCount(a.word,  a.count + b.count);
//					}
//				});

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);

		env.execute("Socket Window WordCount");
	}


	public static class Splitter implements FlatMapFunction<String, WordWithCount> {
        @Override
        public void flatMap(String value, Collector<WordWithCount> out) {
            String[] tokens = value.split("\\W+");
            long eventTime = Long.parseLong(tokens[1]);
            out.collect(new WordWithCount(tokens[0], eventTime, 1));
        }
    }
	public static class AssignTimeStampAndWaterMark implements AssignerWithPeriodicWatermarks<WordWithCount> {

	    private long currentMaxTimestamp = 0l;

	    // 10 seconds lateness
	    private final long maxOutOfOrderness = 10000l;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            long waterMark = currentMaxTimestamp - maxOutOfOrderness;
            System.out.println("wall clock: " + System.currentTimeMillis() + " new WaterMark: " + waterMark);
            return new Watermark(waterMark);
        }

        @Override
        public long extractTimestamp(WordWithCount element, long previousElementTimestamp) {
            long timestamp = element.timestamp;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println("Get element timestamp: " + timestamp + " currentMaxTimestamp: " + currentMaxTimestamp);
            return timestamp;
        }
    }
	// ------------------------------------------------------------------------

	/**
	 * Data type for words with count.
	 */
	public static class WordWithCount {

		public String word;
		public long timestamp;
		public long count;

		public WordWithCount() {}

		public WordWithCount(String word, long timestamp, long count) {
			this.word = word;
			this.timestamp = timestamp;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + timestamp + " : " + count;
		}
	}
}
