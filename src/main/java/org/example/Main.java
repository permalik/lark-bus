package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.*;
import java.util.logging.*;

public class Main {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        try {
            FileOutputStream fos = new FileOutputStream(
                "/Users/tymalik/Docs/Git/lark-saga-bus-consumer/logs/out.log",
                true
            ); // append=true
            PrintStream ps = new PrintStream(fos, true);
            System.setOut(ps);
            System.setErr(ps);

            setupLogger();
        } catch (IOException e) {
            System.err.println(
                "Failed to redirect output streams: " + e.getMessage()
            );
        }

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

    private static void setupLogger() throws IOException {
        Logger logger = Logger.getLogger("");
        Handler[] handlers = logger.getHandlers();
        for (Handler handler : handlers) {
            logger.removeHandler(handler);
        }

        FileHandler fileHandler = new FileHandler(
            "/Users/tymalik/Docs/Git/lark-saga-bus-consumer/logs/out.log",
            true
        );
        fileHandler.setFormatter(new SimpleFormatter());
        logger.addHandler(fileHandler);
    }
}
