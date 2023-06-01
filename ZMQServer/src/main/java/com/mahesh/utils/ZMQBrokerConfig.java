package com.mahesh.utils;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ZMQBrokerConfig {
    private final Logger logger = LoggerFactory.getLogger(ZMQBrokerConfig.class);
    private final String CONFIG_DIR = System.getProperty("user.dir") + "/configs";
    private final String FILE_NAME = "ZMQBroker.config";
    private final Properties prop;

    private ZMQBrokerConfig(final String configFileDir, final String configFileName) throws IOException {
        this.prop = new Properties();
        try (FileInputStream fis = new FileInputStream((configFileDir == null ? CONFIG_DIR : configFileDir) + "/" + (configFileName == null ? FILE_NAME : configFileName))) {
            prop.load(fis);
        }
        logger.info(prop.toString());
    }

    public static class ZMQBrokerConfigBuilder {
        private String configFileDir;
        private String configFileName;

        public ZMQBrokerConfigBuilder setConfigFileDir(String configFileDir) {
            this.configFileDir = configFileDir;
            return this;
        }

        public ZMQBrokerConfigBuilder setConfigFileName(String configFileName) {
            this.configFileName = configFileName;
            return this;
        }

        @SneakyThrows
        public ZMQBrokerConfig build() {
            return new ZMQBrokerConfig(configFileDir, configFileName);
        }
    }

    public final String getProperty(String key){
        return prop.getProperty(key);
    }

}
