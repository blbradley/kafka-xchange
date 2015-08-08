package kafka.xchange;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
    private final Logger logger = LoggerFactory.getLogger(Config.class);
    private static Config config;
    Properties producerProps = new Properties();
    Properties configProps = new Properties();

    private Config() {
        producerProps.put("metadata.broker.list", "localhost:9092");
        producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProps.put("request.required.acks", "1");

        producerProps = readPropertiesFile(producerProps, "config/producer.properties");
        configProps = readPropertiesFile(configProps, "config/config.properties");
    }

    public static synchronized Config getInstance() {
        if (config == null) {
            config = new Config();
        }
        return config;
    }

    public Properties readPropertiesFile(Properties props, String path) {
        try {
            FileInputStream propertiesFile = new FileInputStream(path);
            props.load(propertiesFile);
        } catch (IOException e) {
            logger.error(e.getClass().getName() + ":" + e.getMessage());
        }

        return props;
    }

    public int getPollingPeriod() {
        return Integer.parseInt(configProps.getProperty("polling_period"));
    }
}
