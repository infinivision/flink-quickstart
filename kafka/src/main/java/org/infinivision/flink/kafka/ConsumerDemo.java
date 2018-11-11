package org.infinivision.flink.kafka;

public class ConsumerDemo {
    public static void main(String[] args) {
        String clientId;
        if (args.length == 0) {
            clientId = "Consumer-1";
        } else clientId = args[0];
        Consumer consumerThread = new Consumer(KafkaProperties.TRAIN_TOPIC, clientId);
        consumerThread.start();

    }
}
