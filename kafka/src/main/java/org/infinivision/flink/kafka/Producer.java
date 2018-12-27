package org.infinivision.flink.kafka;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
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

        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        // read csv
        CSVParser csvParser;
        try {
            Reader reader = Files.newBufferedReader(Paths.get(KafkaProperties.TRAIN_PATH));
            csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return;
        }


        int messageNo = 1;

        for (CSVRecord record: csvParser) {
            System.out.println("record number: " + record.getRecordNumber());
            String aid = record.get("aid");
            String uid = record.get("uid");
            String label = record.get("label");
            long ts = System.currentTimeMillis();
            String messageStr = String.join(",", aid, uid, label, Long.toString(ts));
            System.out.println(messageStr);

            ProducerRecord producerRecord = new ProducerRecord<>(topic, messageNo, messageStr);

            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(producerRecord, new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                try {
                    RecordMetadata metadata = (RecordMetadata) producer.send(producerRecord).get();
                    long elapsedTime = System.currentTimeMillis() - startTime;

                    System.out.println("Synchronously Sent message: (" + messageNo + ", " + messageStr
                    + ") offset(" + metadata.offset() + ") partition(" + metadata.partition() + ") in "+ elapsedTime + " ms");

                    metadata = producer.send(new ProducerRecord<>(topic,
                            messageNo,
                            messageStr)).get();

                    System.out.println("Synchronously Sent message: (" + messageNo + ", " + messageStr
                            + ") offset(" + metadata.offset() + ") partition(" + metadata.partition() + ") in "+ elapsedTime + " ms");


                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ++messageNo;
            if (messageNo == 100) {
                return;
            }
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "asynchronously sent message(" + key + ", " + message + ") partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}