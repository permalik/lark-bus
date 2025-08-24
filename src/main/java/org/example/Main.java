package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.example.consumer.SagaConsumer;
import org.example.http.AsyncHttpForwarder;
import org.example.model.Message;
import org.example.producer.SagaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        SagaProducer sagaOutProducer = new SagaProducer("saga.out");
        AsyncHttpForwarder forwarder = new AsyncHttpForwarder(
            "http://localhost:9999",
            sagaOutProducer
        );
        SagaConsumer promptConsumer = new SagaConsumer(Arrays.asList("prompt"));
        SagaConsumer sagaInConsumer = new SagaConsumer(
            Arrays.asList("saga.in")
        );

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    logger.info("Shutting down...");
                    promptConsumer.close();
                    sagaInConsumer.close();
                    sagaOutProducer.close();
                })
            );

        logger.info("Starting async Kafka → HTTP → Kafka service");

        while (true) {
            ConsumerRecords<String, String> promptRecords =
                promptConsumer.consumeAndProcess();
            for (ConsumerRecord<String, String> record : promptRecords) {
                Message msg = mapper.readValue(record.value(), Message.class);
                logger.info(
                    "[PROMPT] Consumed: {}",
                    mapper.writeValueAsString(msg)
                );
                forwarder.forward(msg);
            }

            ConsumerRecords<String, String> sagaRecords =
                sagaInConsumer.consumeAndProcess();
            for (ConsumerRecord<String, String> record : sagaRecords) {
                Message msg = mapper.readValue(record.value(), Message.class);
                logger.info(
                    "[SAGA.IN] Consumed: {}",
                    mapper.writeValueAsString(msg)
                );
                forwarder.forward(msg);
            }

            Thread.sleep(50);
        }
    }
}
