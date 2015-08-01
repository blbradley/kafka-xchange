package kafka.xchange;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import com.xeiam.xchange.Exchange;
import com.xeiam.xchange.ExchangeFactory;
import com.xeiam.xchange.service.polling.marketdata.PollingMarketDataService;

public class KafkaXchange {
    static final Logger logger = LoggerFactory.getLogger(KafkaXchange.class);
    static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) {
        Config config = Config.getInstance();

        ProducerConfig producerConfig = new ProducerConfig(config.producerProps);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);

        String configuredExchangesProp = config.configProps.getProperty("exchanges.active");
        List<String> configuredExchanges = Arrays.asList(configuredExchangesProp.split(","));

        Iterator<Exchange> loadedExchangesIterator = ExchangeProvider.getInstance().getExchanges();
        List<Exchange> loadedExchanges = new ArrayList<Exchange>();
        List<String> loadedExchangeNames = new ArrayList<String>();
        while(loadedExchangesIterator.hasNext()) {
            Exchange loadedExchangeClass = loadedExchangesIterator.next();
            Exchange loadedExchange = ExchangeFactory.INSTANCE.createExchange(loadedExchangeClass.getClass().getName());
            String loadedExchangeName = loadedExchange.getExchangeSpecification().getExchangeName().toLowerCase();
            loadedExchangeNames.add(loadedExchangeName);
            loadedExchanges.add(loadedExchange);
        }

        Iterator<String> configuredExchangeIterator = configuredExchanges.iterator();
        while(configuredExchangeIterator.hasNext()){
            String configuredExchange = configuredExchangeIterator.next();
            if(!loadedExchangeNames.contains(configuredExchange)){
                logger.warn("exchanges.active has an invalid exchange: " + configuredExchange);
            }
        }

        loadedExchangesIterator = loadedExchanges.iterator();
        while(loadedExchangesIterator.hasNext()) {
            Exchange loadedExchange = loadedExchangesIterator.next();
            String loadedExchangeName = loadedExchange.getExchangeSpecification().getExchangeName().toLowerCase();
            if (!configuredExchanges.contains(loadedExchangeName)) {
                continue;
            }

            PollingMarketDataService marketDataService = loadedExchange.getPollingMarketDataService();
            TickerProducerRunnable tickerProducer = new TickerProducerRunnable(marketDataService, loadedExchangeName, producer);
            try {
                ScheduledFuture<?> tickerProducerHandler =
                  scheduler.scheduleAtFixedRate(tickerProducer, 0, 10, SECONDS);
            } catch (Exception e) {
                tickerProducer.close();
                throw new RuntimeException(e);
            }
        }
    }

}
