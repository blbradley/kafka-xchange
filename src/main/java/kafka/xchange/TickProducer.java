package kafka.xchange;

import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import static java.util.concurrent.TimeUnit.*;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.RuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.xeiam.xchange.Exchange;
import com.xeiam.xchange.ExchangeFactory;
import com.xeiam.xchange.service.polling.marketdata.PollingMarketDataService;
import com.xeiam.xchange.dto.marketdata.Ticker;
import com.xeiam.xchange.currency.CurrencyPair;
import com.xeiam.xchange.utils.DateUtils;

import kafka.xchange.ExchangeProvider;


class TickerProducerRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TickerProducerRunnable.class);
    private PollingMarketDataService marketDataService;
    private String loadedExchangeName;
    private String topicName;
    private Producer<String, String> producer;

    TickerProducerRunnable(PollingMarketDataService marketDataService,
               String loadedExchangeName,
               Producer<String, String> producer) {
        this.marketDataService = marketDataService;
        this.loadedExchangeName = loadedExchangeName;
        this.topicName = "ticks";
        this.producer = producer;
    }

    public void run() {
        Ticker ticker = null;
        try {
            ticker = this.marketDataService.getTicker(CurrencyPair.BTC_USD);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String msg = TickProducer.tickerToJSON(ticker).toString();
        logger.debug("Preparing message for topic " +  "-> " + this.loadedExchangeName + ":" + msg);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(this.topicName, this.loadedExchangeName, msg);
        this.producer.send(data);
    }

    public void close() {
        this.producer.close();
    }
}


public class TickProducer {
    private static final Logger logger = LoggerFactory.getLogger(TickProducer.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        FileInputStream producerConfigFile = new FileInputStream("config/producer.properties");
        props.load(producerConfigFile);
 
        ProducerConfig config = new ProducerConfig(props);

        FileInputStream configFile = new FileInputStream("config/config.properties");
        props.load(configFile);

        String configuredExchangesProp = props.getProperty("exchanges.active");
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

            Producer<String, String> producer = new Producer<String, String>(config);

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

    public static JSONObject tickerToJSON(Ticker ticker) {
        JSONObject json = new JSONObject();

        json.put("pair", ticker.getCurrencyPair());
        json.put("last", ticker.getLast());
        json.put("bid", ticker.getBid());
        json.put("ask", ticker.getAsk());
        json.put("high", ticker.getHigh());
        json.put("low", ticker.getLow());
        json.put("avg", ticker.getVwap());
        json.put("volume", ticker.getVolume());
        json.put("timestamp", DateUtils.toMillisNullSafe(ticker.getTimestamp()));

        return json;
    }
}
