package org.infinivision.flink.examples.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaConsumer {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);

//        env.setStateBackend()
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.19.0.108:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");


        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>(
                "test", new SimpleStringSchema(), props);

        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);

    }
}
