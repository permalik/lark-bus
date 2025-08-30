package org.example.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.example.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class JavaHttpReceiver {

    private static final Logger logger = LoggerFactory.getLogger(JavaHttpReceiver.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void start(int port) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/receive", (HttpExchange exchange) -> {
            if ("POST".equals(exchange.getRequestMethod())) {
                InputStream is = exchange.getRequestBody();
                Message msg = mapper.readValue(is, Message.class);
                logger.info("[JAVA RECEIVE] Got message: {}", mapper.writeValueAsString(msg));

                // respond 200
                String response = mapper.writeValueAsString(msg);
                exchange.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else {
                exchange.sendResponseHeaders(400, 0);
                exchange.getResponseBody().close();
            }
        });
        server.start();
        logger.info("Java receiver running on port {}", port);
    }


}
