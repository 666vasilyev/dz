package org.parsers.Config;

public record Config (
        CommonConfig common,
    RabbitmqConfig rabbitmq,
    ParserConfig parser,
    ElasticsearchConfig elasticsearch
){}
