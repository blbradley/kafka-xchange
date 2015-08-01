package kafka.xchange;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.*;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.RuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import com.xeiam.xchange.service.polling.marketdata.PollingMarketDataService;
import com.xeiam.xchange.dto.marketdata.Ticker;
import com.xeiam.xchange.currency.CurrencyPair;
import com.xeiam.xchange.utils.DateUtils;


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
        String msg = tickerToJSON(ticker).toString();
        logger.debug("Preparing message for topic " +  "-> " + this.loadedExchangeName + ":" + msg);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(this.topicName, this.loadedExchangeName, msg);
        this.producer.send(data);
    }

    public void close() {
        this.producer.close();
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
