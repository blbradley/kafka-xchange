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
    private PollingMarketDataService marketDataService;
    private String topicName;
    private Producer<String, String> producer;

    TickerProducerRunnable(PollingMarketDataService marketDataService,
               String topicName,
               Producer<String, String> producer) {
        this.marketDataService = marketDataService;
        this.topicName = topicName;
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
        System.out.println(this.topicName + ':' + msg);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(this.topicName, msg);
        this.producer.send(data);
    }

    public void close() {
        this.producer.close();
    }
}

public class TickProducer {
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

        String activeExchangesProp = props.getProperty("exchanges.active");
        List<String> activeExchanges = Arrays.asList(activeExchangesProp.split(","));

        Iterator<Exchange> exchanges = ExchangeProvider.getInstance().getExchanges();
        while(exchanges.hasNext()) {
            Exchange exchangeClass = exchanges.next();
            Exchange exchange = ExchangeFactory.INSTANCE.createExchange(exchangeClass.getClass().getName());

            String exchangeName = exchange.getExchangeSpecification().getExchangeName().toLowerCase();
            if (!activeExchanges.contains(exchangeName)) {
                break;
            }

            PollingMarketDataService marketDataService = exchange.getPollingMarketDataService();
            String topicName = exchangeName + "-ticks";

            Producer<String, String> producer = new Producer<String, String>(config);

            TickerProducerRunnable tickerProducer = new TickerProducerRunnable(marketDataService, topicName, producer);
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
