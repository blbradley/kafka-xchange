package kafka.xchange;

import java.io.IOException;
import java.lang.RuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import kafka.javaapi.producer.Producer;

import com.xeiam.xchange.Exchange;
import com.xeiam.xchange.dto.marketdata.Ticker;
import com.xeiam.xchange.currency.CurrencyPair;

interface TickerTopic {
    static String topicName = "ticks";
}

class TickerProducerRunnable extends PollingExchangeProducerRunnable implements TickerTopic, ObjectMapperInjector {
    private static final Logger logger = LoggerFactory.getLogger(TickerProducerRunnable.class);

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
        String msg;
        try {
            msg = mapper.writeValueAsString(ticker);
            send(TickerProducerRunnable.topicName, msg);
        } catch (JsonProcessingException e) {
            logger.error(e.getClass().getName() + ":" + e.getMessage());
        }
    }
}
