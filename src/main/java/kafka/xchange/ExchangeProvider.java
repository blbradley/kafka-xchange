package kafka.xchange;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceConfigurationError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xeiam.xchange.Exchange;
import com.xeiam.xchange.ExchangeFactory;

public class ExchangeProvider {

    private final Logger logger = LoggerFactory.getLogger(ExchangeProvider.class);
    private static List<Exchange> exchanges = new ArrayList<Exchange>();
    private static ExchangeProvider provider;

    private ExchangeProvider() {
        ServiceLoader<Exchange> loader = ServiceLoader.load(Exchange.class);

        Iterator<Exchange> exchangesIterator = null;
        try {
            exchangesIterator = loader.iterator();
        }
        catch (ServiceConfigurationError serviceError) {
            serviceError.printStackTrace();
        }

        if(logger.isDebugEnabled()) {
            // log each loaded exchange
            for(; exchangesIterator.hasNext();) {
                logger.debug(exchangesIterator.next() + " loaded by ServiceLoader");
            }
            // refresh iterator exhausted above
            exchangesIterator = loader.iterator();
        }

        while(exchangesIterator.hasNext()) {
            Exchange exchangeClass = exchangesIterator.next();
            Exchange exchange = ExchangeFactory.INSTANCE.createExchange(exchangeClass.getClass().getName());
            exchanges.add(exchange);
        }
    }

    public static synchronized ExchangeProvider getInstance() {
        if (provider == null) {
            provider = new ExchangeProvider();
        }
        return provider;
    }

    public Iterator<Exchange> getExchanges() {
        return exchanges.iterator();
    }
}
