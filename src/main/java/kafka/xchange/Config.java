package kafka.xchange;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xeiam.xchange.Exchange;

public class Config {
    private final Logger logger = LoggerFactory.getLogger(Config.class);
    private Map<String, Exchange> loadedExchanges = new ConcurrentHashMap<String,Exchange>();
    private int pollingPeriod;

    Config(Properties props) {
        Iterator<Exchange> loadedExchangesIterator = ExchangeProvider.getInstance().getExchanges();
        while(loadedExchangesIterator.hasNext()) {
            Exchange loadedExchange = loadedExchangesIterator.next();
            String loadedExchangeName = loadedExchange.getExchangeSpecification().getExchangeName().toLowerCase();
            loadedExchanges.put(loadedExchangeName, loadedExchange);
        }

        List<String> configuredExchanges = Arrays.asList(props.getProperty("exchanges.active").split(","));
        Iterator<String> configuredExchangeIterator = configuredExchanges.iterator();
        while(configuredExchangeIterator.hasNext()){
            String configuredExchange = configuredExchangeIterator.next();
            if(!loadedExchanges.keySet().contains(configuredExchange)){
                logger.warn("exchanges.active has an invalid exchange: " + configuredExchange);
            }
        }

        // delete unconfigured exchanges from loaded exchanges
        Iterator<String> loadedExchangeNamesIterator = loadedExchanges.keySet().iterator();
        while(loadedExchangeNamesIterator.hasNext()){
            String loadedExchangeName = loadedExchangeNamesIterator.next();
            if (!configuredExchanges.contains(loadedExchangeName)) {
                loadedExchanges.remove(loadedExchangeName);
            }
        }

        pollingPeriod = Integer.parseInt(props.getProperty("polling_period"));
        if (pollingPeriod <= 0) {
            throw new Error("polling_period should be greater than zero.");
        }
    }

    public Collection<Exchange> getExchanges() {
        return loadedExchanges.values();
    }

    public int getPollingPeriod() {
        return pollingPeriod;
    }
}
