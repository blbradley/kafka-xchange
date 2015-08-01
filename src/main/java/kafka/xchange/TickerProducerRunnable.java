package kafka.xchange;

import org.json.JSONObject;

import java.io.IOException;
import java.lang.RuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;

import com.xeiam.xchange.Exchange;
import com.xeiam.xchange.dto.marketdata.Ticker;
import com.xeiam.xchange.currency.CurrencyPair;
import com.xeiam.xchange.utils.DateUtils;

interface TickerTopic {
    static String topicName = "ticks";
}

class TickerProducerRunnable extends PollingExchangeProducerRunnable implements TickerTopic {

    TickerProducerRunnable(Producer<String, String> producer,
            Exchange exchange) {
        super(producer, exchange);
    }

    public void run() {
        Ticker ticker = null;
        try {
            ticker = this.marketDataService.getTicker(CurrencyPair.BTC_USD);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String msg = tickerToJSON(ticker).toString();
        send(TickerProducerRunnable.topicName, msg);
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
