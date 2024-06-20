package org.parsers.Adapters;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.parsers.Config.ElasticsearchConfig;
import org.parsers.DTO.NewsRecordDTO;

import java.io.IOException;

public class ElasticAdapter {
    private static final Logger logger = LogManager.getLogger(ElasticAdapter.class);
    private final ElasticsearchClient client;
    private final ElasticsearchConfig config;

    public ElasticAdapter(ElasticsearchClient client, ElasticsearchConfig elasticsearchConfig) {
        this.client = client;
        this.config = elasticsearchConfig;
    }

    public void init() throws IOException {
        if (!client.indices().exists(i -> i.index(config.indexName())).value()) {
            createIndex();
        }
    }


    private void createIndex() throws IOException {
        client.indices().create(i -> i.index(config.indexName())
                .mappings(m -> m.properties("hash", p -> p.keyword(d -> d))
                        .properties("link", p -> p.text(d -> d))
                        .properties("category", p -> p.text(d -> d))
                        .properties("title", p -> p.text(d -> d))
                        .properties("content", p -> p.keyword(d -> d))
                ));
        logger.info("Index {} created successfully", config.indexName());
    }

    public boolean checkDocumentExists(NewsRecordDTO record) throws IOException {
        SearchResponse<Object> searchResponse = client.search(builder ->
                        builder.index(config.indexName())
                                .query(query -> query.term(termQuery -> termQuery.field("hash").value(record.hash()))),
                Object.class);

        assert searchResponse.hits().total() != null;
        return searchResponse.hits().total().value() != 0;
    }

    public void addDocument(NewsRecordDTO record) {
        try {
            client.index(index -> index
                    .index(config.indexName())
                    .document(record));
            logger.info("Document indexed successfully with hash: {}", record.hash());
        } catch (IOException e) {
            logger.error("Error indexing document with hash {}", record.hash(), e);
        }
    }
}