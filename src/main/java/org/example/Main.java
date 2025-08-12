package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        SagaPromptRawConsumer consumer = new SagaPromptRawConsumer(
            "saga.prompt.raw"
        );

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    logger.info("Shutting down..");
                    consumer.close();
                })
            );

        logger.info("Starting Saga Bus Consumer..");

        try {
            while (true) {
                String rawMsg = consumer.consumeAndProcess();
                if (rawMsg != null) {
                    Message message = mapper.readValue(rawMsg, Message.class);

                    logger.info(
                        "Received Message: " +
                        mapper.writeValueAsString(message)
                    );

                    String json = mapper.writeValueAsString(message);

                    HttpClient client = HttpClient.newHttpClient();
                    HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:9999"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();
                    HttpResponse<String> response = client.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                    );
                    logger.info(
                        "Forwarded to :9999, Response code: " +
                        response.statusCode()
                    );
                }

                Thread.sleep(100);
            }
        } catch (InterruptedException | IOException e) {
            logger.error("Interrupted or I/O error: " + e.getMessage());
        }
    }
}
