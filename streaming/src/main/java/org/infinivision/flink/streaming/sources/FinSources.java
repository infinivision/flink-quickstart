package org.infinivision.flink.streaming.sources;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.infinivision.flink.streaming.entity.Trade;

public class FinSources {
    /**
     * This source generates the stream of trades
     *
     * */
    public static DataStream<Trade> tradeSource(StreamExecutionEnvironment env) {
        // This is a bit of a hack to use Thread.sleep() for sequencing but it works for our test purposes
        DataStream<Trade> trades = env.addSource(new SourceFunction<Trade>() {
            private volatile boolean running = true;

            @Override
            public void run(SourceContext<Trade> sc) throws Exception {
                Thread.sleep(1000);
                sc.collectWithTimestamp(new Trade(10000L, 0L, "trade-1000"), 10000);
                sc.emitWatermark(new Watermark(10000));
                Thread.sleep(3000);
                sc.collectWithTimestamp(new Trade(11000L, 0L, "trade-1200"), 11000);
                sc.collectWithTimestamp(new Trade(12000L, 0L, "trade-1200"), 12000);
                sc.emitWatermark(new Watermark(12000));
                Thread.sleep(1000);
                sc.collectWithTimestamp(new Trade(15000L, 0L, "trade-1500"), 15000);
                sc.emitWatermark(new Watermark(15000));
                Thread.sleep(1000);
                sc.collectWithTimestamp(new Trade(17000L, 0L, "trade-1700"), 17000);
                sc.emitWatermark(new Watermark(1700));
                Thread.sleep(1000);
                sc.collectWithTimestamp(new Trade(18000L, 0L, "trade-1800"), 18000);
                sc.emitWatermark(new Watermark(18000));
                Thread.sleep(1000);
                sc.collectWithTimestamp(new Trade(20000L, 0L, "trade-2000"), 20000);
                sc.emitWatermark(new Watermark(24998));
                sc.collectWithTimestamp(new Trade(24999L, 0L, "trade-2000"), 24999);


                while (running) {
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        return trades;
    }
}
