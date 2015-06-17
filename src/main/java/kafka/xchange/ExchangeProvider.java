package kafka.xchange;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.ServiceConfigurationError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xeiam.xchange.Exchange;

public class ExchangeProvider {

    private final Logger logger = LoggerFactory.getLogger(ExchangeProvider.class);
    private static ExchangeProvider provider;
    private ServiceLoader<Exchange> loader;

    private ExchangeProvider() {
        loader = ServiceLoader.load(Exchange.class);
    }

    public static synchronized ExchangeProvider getInstance() {
        if (provider == null) {
            provider = new ExchangeProvider();
        }
        return provider;
    }

    public Iterator<Exchange> getExchanges() {
        Iterator<Exchange> exchanges = null;
        try {
            exchanges = loader.iterator();
        }
        catch (ServiceConfigurationError serviceError) {
            serviceError.printStackTrace();
        }
        if(logger.isDebugEnabled()) {
            // log each loaded exchange
            for(; exchanges.hasNext();) {
                logger.debug(exchanges.next() + " loaded by ServiceLoader");
            }
            // refresh iterator for return
            exchanges = loader.iterator();
        }
        return exchanges;
    }
}
