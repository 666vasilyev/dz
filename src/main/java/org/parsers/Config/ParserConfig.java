package org.parsers.Config;

public record ParserConfig(String baseUrl,
                           String queue,
                           String linkQueue,
                           String resultQueue) {
}
