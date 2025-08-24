package org.example.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SagaConsumer {

    private final KafkaConsumer<String, String> consumer;
    private static final Logger logger = LoggerFactory.getLogger(
        SagaConsumer.class
    );

    public SagaConsumer(List<String> topics) {
        logger.info("Initializing consumer..");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "lark-saga-bus");
        props.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName()
        );
        props.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName()
        );

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        logger.info("Subscribed to topics: {}", topics);
    }

    public ConsumerRecords<String, String> consumeAndProcess() {
        return consumer.poll(Duration.ofMillis(200));
    }

    public void close() {
        consumer.close();
    }
}
