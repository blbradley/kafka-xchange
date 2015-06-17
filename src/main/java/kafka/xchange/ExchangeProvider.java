package kafka.xchange;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.ServiceConfigurationError;

import com.xeiam.xchange.Exchange;

public class ExchangeProvider {

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
        try {
            return loader.iterator();
        }
        catch (ServiceConfigurationError serviceError) {
            serviceError.printStackTrace();
            return null;
        }
    }
}
