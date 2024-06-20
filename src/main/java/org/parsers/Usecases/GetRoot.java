package org.parsers.Usecases;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.parsers.Config.ParserConfig;
import org.parsers.DTO.NewsDTO;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class GetRoot implements Runnable {
    private final ConnectionFactory connectionFactory;
    private final ParserConfig parserConfig;
    private static final Logger logger = LogManager.getLogger(GetRoot.class);

    public GetRoot(ConnectionFactory factory, ParserConfig parser) {
        this.connectionFactory = factory;
        this.parserConfig = parser;
    }

    @Override
    public void run() {
        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel();
             var client = HttpClient.newHttpClient()) {
            var req = HttpRequest.newBuilder().GET().uri(URI.create(parserConfig.baseUrl())).build();
            var resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());

            var objectMapper = new ObjectMapper();

            var xmlMapper = new XmlMapper();
            var root = xmlMapper.readTree(resp.body());
            for (var news : root.get("channel").get("item")) {
                var converted = NewsDTO.fromLink(news.get("link").asText());
                channel.basicPublish("", parserConfig.linkQueue(), null, objectMapper.writeValueAsBytes(converted));
            }

        } catch (NoSuchAlgorithmException | IOException | TimeoutException | InterruptedException e) {
            logger.error(e);
        }
    }
}
