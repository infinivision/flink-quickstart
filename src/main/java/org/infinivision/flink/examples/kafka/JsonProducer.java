package org.infinivision.flink.examples.kafka;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class JsonProducer {

    public static void main(String[] args) {
        KafkaProducer<Integer, String> producer;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // enable idemptence
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, "2");

        producer = new KafkaProducer<>(props);


        // read csv
        CSVParser csvParser;
        try {
            Reader reader = Files.newBufferedReader(Paths.get(KafkaProperties.AD_FEATURE_PATH));
            csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return;
        }

        for (CSVRecord record: csvParser) {
            System.out.println("record number: " + record.getRecordNumber());
            int aid = Integer.parseInt(record.get("aid"));
            int advertiserId = Integer.parseInt(record.get("advertiserId"));
            int campaignId = Integer.parseInt(record.get("campaignId"));
            int creativeId = Integer.parseInt(record.get("creativeId"));
            int creativeSize = Integer.parseInt(record.get("creativeSize"));
            int adCategoryId = Integer.parseInt(record.get("adCategoryId"));
            int productId = Integer.parseInt(record.get("productId"));
            int productType = Integer.parseInt(record.get("productType"));

            JSONObject obj = new JSONObject();
            obj.put("aid", aid);
            obj.put("advertiserId", advertiserId);
            obj.put("campaignId", campaignId);
            obj.put("creativeId", creativeId);
            obj.put("creativeSize", creativeSize);
            obj.put("adCategoryId", adCategoryId);
            obj.put("productId", productId);
            obj.put("productType", productType);

            String message = obj.toString();
            System.out.println("message: " + message);

            ProducerRecord producerRecord = new ProducerRecord<>(KafkaProperties.AD_FEATURE_TOPIC, message);
            long startTime = System.currentTimeMillis();

            try {
                RecordMetadata metadata = (RecordMetadata) producer.send(producerRecord).get();

                long elapsedTime = System.currentTimeMillis() - startTime;

                System.out.println("Synchronously Sent message: (" + message
                        + ") offset(" + metadata.offset() + ") partition(" + metadata.partition() + ") in "+ elapsedTime + " ms");

                Thread.sleep(1000);

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }


        }
    }

}
