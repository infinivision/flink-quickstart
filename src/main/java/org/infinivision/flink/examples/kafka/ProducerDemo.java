package org.infinivision.flink.examples.kafka;

public class ProducerDemo {
    public static void main(String[] args) {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Producer producerThread = new Producer(KafkaProperties.TRAIN_TOPIC, isAsync);
        producerThread.start();
    }
}
