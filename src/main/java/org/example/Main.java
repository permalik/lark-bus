package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;

public class Main {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        SagaPromptRawConsumer consumer = new SagaPromptRawConsumer(
            "saga.prompt.raw"
        );

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.println("Shutting down..");
                    consumer.close();
                })
            );

        System.out.println("Starting Saga Bus Consumer..");

        try {
            while (true) {
                String rawMsg = consumer.consumeAndProcess();
                if (rawMsg != null) {
                    Message message = mapper.readValue(rawMsg, Message.class);

                    System.out.println("Received Message:");
                    System.out.println("  msgId: " + message.msgId);
                    System.out.println("  content: " + message.content);

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
                    System.out.println(
                        "Forwarded to :9999, Response code: " +
                        response.statusCode()
                    );
                }

                Thread.sleep(100);
            }
        } catch (InterruptedException | IOException e) {
            System.err.println("Interrupted or I/O error: " + e.getMessage());
        }
    }
}
