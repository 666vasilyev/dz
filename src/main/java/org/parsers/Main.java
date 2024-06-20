package org.parsers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.parsers.Adapters.ElasticAdapter;
import org.parsers.Config.Config;
import org.parsers.Config.ElasticsearchConfig;
import org.parsers.Usecases.GetRoot;
import org.parsers.Usecases.Parser;
import org.parsers.Usecases.SaveToElastic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static Config readConfig(URL configPath) throws IOException {
        var mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(configPath, Config.class);
    }

    private static void initQueues(ConnectionFactory factory, String[] queues) throws IOException, TimeoutException {
        try (var connection = factory.newConnection(); var channel = connection.createChannel()) {
            for (String queue : queues) {
                logger.info("Creating queue {}", queue);
                channel.queueDeclare(queue, true, false, false, null);
            }
        }
    }

    private static ElasticsearchClient initElastic(ElasticsearchConfig config) {
        RestClient restClient = RestClient
                .builder(HttpHost.create(config.host()))
                .setDefaultHeaders(new Header[]{
                        new BasicHeader("Authorization", "ApiKey " + config.apiKey())
                })
                .build();

        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        return new ElasticsearchClient(transport);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Start app");
        // Чтение конфига
        var config = readConfig(Main.class.getClassLoader().getResource("config.yaml"));

        // Настройка фабрики соединений с RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.rabbitmq().host());
        factory.setPort(config.rabbitmq().port());
        factory.setVirtualHost("/");
        factory.setUsername(config.rabbitmq().username());
        factory.setPassword(config.rabbitmq().password());

        initQueues(factory, new String[]{
                config.parser().queue(),
                config.parser().linkQueue(),
                config.parser().resultQueue()
        });

        // Сбор ссылок с основной страницы
        logger.info("Starting collection of the news links");

        // Парсинг информации по каждой ссылке
        logger.info("Parser workers init");
        try (ExecutorService executorService = Executors.newFixedThreadPool(config.common().threads())) {
            executorService.submit(new GetRoot(factory, config.parser()));

            for (int i = 0; i < config.common().threads() - 2; i++) {
                executorService.submit(new Parser(factory, config.parser()));
            }

            logger.info("The collection of information from each URL page is completed");
            ElasticsearchClient esClient = initElastic(config.elasticsearch());

            var elasticAdapter = new ElasticAdapter(esClient, config.elasticsearch());
            elasticAdapter.init();

            executorService.submit(new SaveToElastic(factory, config.parser().resultQueue(), elasticAdapter));

            var result = executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            if (!result) {
                throw new TimeoutException();
            }
            System.out.println("App stopped");
        }
    }
}