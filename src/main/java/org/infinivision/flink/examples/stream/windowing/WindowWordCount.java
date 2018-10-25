
package org.infinivision.flink.examples.stream.windowing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.infinivision.flink.examples.stream.wordcount.util.WordCountData;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt; --window &lt;n&gt; --slide &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>use basic windowing abstractions.
 * </ul>
 */
public class WindowWordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

        // the host and the port to connect to
        final String hostname = params.get("hostname", "localhost");

        final int port = params.getInt("port", 9999);


		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

        DataStream<String> input = env.socketTextStream(hostname, port, "\n");

        final int windowSize = params.getInt("window", 5);
		final int slideSize = params.getInt("slide", 2);

		DataStream<Tuple2<String, Integer>> counts =
		// split up the lines in pairs (2-tuples) containing: (word,1)
		input.flatMap(new Tokenizer())
				// create windows of windowSize records slided every slideSize records
				.keyBy(f -> f.f0)
				.countWindow(windowSize, slideSize)
				// group by the tuple field "0" and sum up tuple field "1"
                .reduce(new ReduceWords(), new ApplyWindowFunction());
//				.sum(1);

		// emit result
        counts.print();

		// execute program
		env.execute("Socket Count Window WordCount");
	}


	public static final class ApplyWindowFunction implements WindowFunction<Tuple2<String, Integer>,
            Tuple2<String, Integer>, String, GlobalWindow> {
        @Override
        public void apply(String s, GlobalWindow window, Iterable<Tuple2<String, Integer>> input,
                          Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("Enter Window Function");
            for (Tuple2<String, Integer> in : input) {
                System.out.println("input Element: " + in.toString());
                out.collect(in);
            }

            System.out.println("Leaving Window Function");
        }
    }


	public static final class ReduceWords implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            System.out.println("Enter Reduce Function");
            System.out.println("value1: " + value1.toString());
            System.out.println("value2: " + value2.toString());
            System.out.println("Leave Reduce Function");

            return new Tuple2<>(value1.f0, value1.f1+value2.f1);
        }
    }
    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] wordCount = value.toLowerCase().split("\\W+");

            out.collect(new Tuple2<>(wordCount[0], Integer.parseInt(wordCount[1])));
        }
    }
}
