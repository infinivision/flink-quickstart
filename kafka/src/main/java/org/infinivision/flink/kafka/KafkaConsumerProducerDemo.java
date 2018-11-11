package org.infinivision.flink.kafka;

public class KafkaConsumerProducerDemo {
    public static void main(String[] args) {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Producer producerThread = new Producer(KafkaProperties.TRAIN_TOPIC, isAsync);
        producerThread.start();

        String clientId;
        if (args.length == 1) {
            clientId = "Consumer-1";
        } else clientId = args[1];
        Consumer consumerThread = new Consumer(KafkaProperties.TRAIN_TOPIC, clientId);
        consumerThread.start();

    }
}
