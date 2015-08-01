package kafka.xchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import com.xeiam.xchange.Exchange;
import com.xeiam.xchange.service.polling.marketdata.PollingMarketDataService;

abstract class PollingExchangeProducerRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PollingExchangeProducerRunnable.class);
    protected Producer<String, String> producer;
    protected Exchange exchange;
    protected String exchangeName;
    protected PollingMarketDataService marketDataService;

    PollingExchangeProducerRunnable(Producer<String, String> producer,
                                    Exchange exchange) {
        this.producer = producer;
        this.exchange = exchange;
        this.exchangeName = exchange.getExchangeSpecification().getExchangeName().toLowerCase();
        this.marketDataService = exchange.getPollingMarketDataService();
    }

    public final void send(String topicName, String msg) {
        logger.debug("Preparing message for topic " + topicName +  "-> " + this.exchangeName + ":" + msg);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, this.exchangeName, msg);
        this.producer.send(data);
    }

    public final void close() {
        this.producer.close();
    }
}
