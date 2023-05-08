package com.mahesh.publisher.Kafka.KafkaClient;

public enum AutoOffsetResetConfig {
    LATEST("latest"),
    EARLIEST("earliest"),
    NONE("none");

    private final String name;

    AutoOffsetResetConfig(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
