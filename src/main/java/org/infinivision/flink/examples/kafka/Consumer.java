package org.infinivision.flink.examples.kafka;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.*;

public class Consumer extends ShutdownableThread {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    public Consumer(String topic, String clientId) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;


//        List<TopicPartition> partitions = Arrays.asList(
//                new TopicPartition(this.topic, 0),
//                new TopicPartition(this.topic, 1),
//                new TopicPartition(this.topic, 2)
//
//        );
//        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
//        for(Map.Entry<TopicPartition, Long> entry: beginningOffsets.entrySet()) {
//            TopicPartition tp = entry.getKey();
//            Long offset = entry.getValue();
//            System.out.println("topic: " + tp.topic() + " partition: " + tp.partition() + " begin offset: " + offset);
//        }
//
//        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
//        for(Map.Entry<TopicPartition, Long> entry: endOffsets.entrySet()) {
//            TopicPartition tp = entry.getKey();
//            Long offset = entry.getValue();
//            System.out.println("topic: " + tp.topic() + " partition: " + tp.partition() + " end offset: " + offset);
//        }
//
//        // specify the partition to consume
//        consumer.assign(Arrays.asList(new TopicPartition(this.topic, 0)));
//        consumer.seekToBeginning(Arrays.asList(new TopicPartition(this.topic, 0)));
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));

        ConsumerRecords<Integer, String> records = consumer.poll(1L);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value()
                    + ") at offset " + record.offset() + " at partition " + record.partition());
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}