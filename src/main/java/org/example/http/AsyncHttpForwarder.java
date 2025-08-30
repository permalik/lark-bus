package org.example.http;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.*;
import java.util.concurrent.CompletableFuture;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.example.model.Message;
import org.example.producer.SagaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.io.OutputStream;

public class AsyncHttpForwarder {

    private static final Logger logger = LoggerFactory.getLogger(
        AsyncHttpForwarder.class
    );
    private static final ObjectMapper mapper = new ObjectMapper();

    private final HttpClient client;
    private final String endpoint;
    private final SagaProducer sagaOutProducer;

    public AsyncHttpForwarder(String endpoint, SagaProducer sagaOutProducer) {
        this.client = HttpClient.newHttpClient();
        this.endpoint = endpoint;
        this.sagaOutProducer = sagaOutProducer;
    }

    public void forward(Message message) {
        try {
            String json = mapper.writeValueAsString(message);

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

            CompletableFuture<HttpResponse<String>> futureResponse =
                client.sendAsync(request, HttpResponse.BodyHandlers.ofString());

            futureResponse
                .thenAccept(response -> {
                    try {
                        String body = response.body();
                        logger.info("RAWWW: {}", body);
                        Message responseMsg = mapper.readValue(
                            response.body(),
                            Message.class
                        );
                        logger.info(
                            "HTTP response received: {}",
                            mapper.writeValueAsString(responseMsg)
                        );

                        sagaOutProducer.produce(
                            mapper.writeValueAsString(responseMsg)
                        );
                    } catch (Exception e) {
                        logger.error(
                            "Failed to handle HTTP response: {}",
                            e.getMessage()
                        );
                    }
                })
                .exceptionally(ex -> {
                    logger.error("HTTP request failed: {}", ex.getMessage());
                    return null;
                });
        } catch (Exception e) {
            logger.error("Failed to forward message: {}", e.getMessage());
        }
    }
}
