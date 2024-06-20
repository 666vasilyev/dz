package org.parsers.Config;

public record RabbitmqConfig(String host,
                             int port,
                             String username,
                             String password) {
}
