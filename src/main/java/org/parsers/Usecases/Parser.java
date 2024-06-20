package org.parsers.Usecases;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import org.apache.http.HttpStatus;
import org.parsers.Config.ParserConfig;
import org.parsers.DTO.NewsDTO;
import org.parsers.DTO.NewsRecordDTO;
import org.parsers.Exceptions.ParsingException;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpRetryException;
import java.net.URI;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


public class Parser implements Runnable {

    private final ConnectionFactory factory;
    private final ParserConfig parserConfig;
    private static final Logger logger = LoggerFactory.getLogger(Parser.class);

    public Parser(ConnectionFactory factory, ParserConfig config) {
        this.factory = factory;
        this.parserConfig = config;
    }

    private NewsRecordDTO parse(InputStream body, String url, String hash) throws IOException, ParsingException {
        var soup = Jsoup.parse(body, "utf-8", "");
        var categories = soup.getElementsByClass("decor");
        assert !categories.isEmpty();
        var category = categories.first();
        assert category != null;

        var headers = soup.getElementsByClass("doc_header__name");
        if (headers.isEmpty()) {
            throw new ParsingException(url);
        }
        var header = headers.first();
        assert header != null;

        var articles = header.getElementsByClass("doc__text");
        if (articles.isEmpty()) {
            throw new ParsingException(url);
        }
        var stringBuilder = new StringBuilder();
        for (var article : articles) {
            stringBuilder.append(article.text());
        }

        return new NewsRecordDTO(
                url,
                category.text(),
                header.text(),
                stringBuilder.toString(),
                hash
        );

    }

    private NewsRecordDTO parseNews(String url, String hash, HttpClient httpClient) throws ParsingException, IOException, InterruptedException {
        try {
            var req = HttpRequest.newBuilder().GET().uri(URI.create(url)).build();
            var resp = httpClient.send(req, HttpResponse.BodyHandlers.ofInputStream());

            return switch (resp.statusCode()) {
                case HttpStatus.SC_OK -> parse(resp.body(), url, hash);
                case HttpStatus.SC_FORBIDDEN -> throw new IOException("HTTP 403: Access is denied");
                case HttpStatus.SC_NOT_FOUND -> throw new IOException("HTTP 404: URL not found");
                case HttpStatus.SC_SERVICE_UNAVAILABLE ->
                        throw new HttpRetryException("HTTP 503: Service unavailable", 20);
                default -> throw new IOException(String.format("HTTP error code: %s", resp.statusCode()));
            };
        } catch (ParsingException | IOException | InterruptedException e) {
            logger.error("Error parsing article from URL: {}", url, e);
            throw e;
        }
    }

    private void handleDelivery(GetResponse delivery, Channel channel, HttpClient httpClient) throws IOException {
        try {
            var mapper = new ObjectMapper();
            var news = mapper.readValue(delivery.getBody(), NewsDTO.class);

            var articleJson = parseNews(news.link(), news.hash(), httpClient);
            String json = articleJson.toString();
            channel.basicPublish("", parserConfig.queue(), null, json.getBytes(StandardCharsets.UTF_8));
            logger.info("Published article info: {}", json);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Exception e) {
            logger.error("Error handling delivery", e);
            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
        }
    }

    @Override
    public void run() {
        try (var connection = factory.newConnection();
             var channel = connection.createChannel();
             var httpClient = HttpClient.newHttpClient()) {

            GetResponse response;
            while ((response = channel.basicGet(parserConfig.linkQueue(), false)) != null) {
                handleDelivery(response, channel, httpClient);
            }
        } catch (IOException | TimeoutException e) {
            logger.error("Error establishing connection", e);
        }
    }
}