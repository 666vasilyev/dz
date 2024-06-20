package org.parsers.Usecases;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.parsers.Adapters.ElasticAdapter;
import org.parsers.DTO.NewsRecordDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SaveToElastic implements Runnable {
    private final ConnectionFactory factory;
    private final String queue;
    private static final Logger logger = LoggerFactory.getLogger(SaveToElastic.class);
    private final ElasticAdapter elasticAdapter;

    public SaveToElastic(ConnectionFactory factory, String queue, ElasticAdapter elasticAdapter) {
        this.factory = factory;
        this.queue = queue;
        this.elasticAdapter = elasticAdapter;
    }

    private void handleDelivery(GetResponse delivery, Channel channel) throws IOException {
        try {
            System.out.println("Saving to elastic: " + delivery.getEnvelope());
            var mapper = new ObjectMapper();
            var record = mapper.readValue(delivery.getBody(), NewsRecordDTO.class);

            var exists = elasticAdapter.checkDocumentExists(record);
            if (!exists) {
                elasticAdapter.addDocument(record);
            }

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Exception e) {
            logger.error("Error handling delivery", e);
            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }

    @Override
    public void run() {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(queue, false, false, false, null);
            GetResponse response;
            while ((response = channel.basicGet(queue, false)) != null) {
                handleDelivery(response, channel);
            }

        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
