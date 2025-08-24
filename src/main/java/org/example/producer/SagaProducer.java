package org.example.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SagaProducer {

    private static final Logger logger = LoggerFactory.getLogger(
        SagaProducer.class
    );
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public SagaProducer(String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName()
        );
        props.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName()
        );

        this.producer = new KafkaProducer<>(props);
    }

    public void produce(String messageJson) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
            topic,
            null,
            messageJson
        );
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error(
                    "Error producing to Kafka: {}",
                    exception.getMessage()
                );
            } else {
                logger.info(
                    "Produced to {}: partition={}, offset={}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset()
                );
            }
        });
    }

    public void close() {
        producer.close();
    }
}
