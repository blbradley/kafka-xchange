package kafka.xchange;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import com.xeiam.xchange.Exchange;

public class KafkaXchange {
    static final Logger logger = LoggerFactory.getLogger(KafkaXchange.class);
    static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) {
        Properties configProps = new Properties();
        configProps = readPropertiesFile(configProps, "config/config.properties");
        Config config = new Config(configProps);

        Properties producerProps = new Properties();
        producerProps.put("metadata.broker.list", "localhost:9092");
        producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProps.put("request.required.acks", "1");

        producerProps = readPropertiesFile(producerProps, "config/producer.properties");

        ProducerConfig producerConfig = new ProducerConfig(producerProps);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);

        Iterator<Exchange> exchangesIterator = config.getExchanges().iterator();
        while(exchangesIterator.hasNext()) {
            Exchange exchange = exchangesIterator.next();
            TickerProducerRunnable tickerProducer = new TickerProducerRunnable(producer, exchange);
            try {
                ScheduledFuture<?> tickerProducerHandler =
                  scheduler.scheduleAtFixedRate(tickerProducer, 0, config.getPollingPeriod(), SECONDS);
            } catch (Exception e) {
                tickerProducer.close();
                throw new RuntimeException(e);
            }
        }
    }

    public static Properties readPropertiesFile(Properties props, String path) {
        try {
            FileInputStream propertiesFile = new FileInputStream(path);
            props.load(propertiesFile);
        } catch (IOException e) {
            logger.error(e.getClass().getName() + ":" + e.getMessage());
        }

        return props;
    }
}
