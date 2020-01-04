package com.yevgen.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record


        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world " + i);
            // send data
            producer.send(record, (recordMetadata, e) -> {
                // executes on successful sent or exception is thrown
                if (e == null) {
                    // record successfully sent
                    logger.info("received new metadata: \n"  +
                            "Topic: "  + recordMetadata.topic() + "\n" +
                            "Partition: "  + recordMetadata.partition() + "\n" +
                            "Offset: "  + recordMetadata.offset() + "\n" +
                            "Timestamp: "  + recordMetadata.timestamp());
                } else {
                    logger.error("Producing error", e);
                }
            });

        }

        // flush data
        producer.flush();
        // flush and close
        producer.close();

    }
}
