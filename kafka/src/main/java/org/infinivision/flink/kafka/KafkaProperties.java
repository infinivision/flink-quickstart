package org.infinivision.flink.kafka;

public class KafkaProperties {
    public static final String TRAIN_TOPIC = "tecent-train";
    public static final String AD_FEATURE_TOPIC = "ad_feature";
    public static final String TRAIN_PATH = "/Users/hongtaozhang/workspace/flink_prj/flink-quickstart/dataset/train.csv";
    public static final String AD_FEATURE_PATH = "/Users/hongtaozhang/workspace/flink_prj/flink-quickstart/dataset/adFeature.csv";
    public static final String KAFKA_SERVER_URL = "172.19.0.108";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    public static final String TOPIC2 = "topic2";
    public static final String TOPIC3 = "topic3";
    public static final String CLIENT_ID = "SimpleConsumerDemoClient";

    private KafkaProperties() {}
}
