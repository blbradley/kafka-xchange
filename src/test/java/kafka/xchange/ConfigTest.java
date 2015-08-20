package kafka.xchange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.core.classloader.annotations.PrepareForTest;

import com.xeiam.xchange.Exchange;

import kafka.xchange.Config;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ExchangeProvider.class)
public class ConfigTest {
    Properties props = new Properties();
    static ExchangeProvider providerMock = mock(ExchangeProvider.class);
    Map<String, Exchange> exchanges = new HashMap<String, Exchange>();

    @BeforeClass
    public static void setUpBeforeClass() {
        PowerMockito.mockStatic(ExchangeProvider.class);
        when(ExchangeProvider.getInstance()).thenReturn(providerMock);
    }

    @Before
    public void setUp() {
        props.setProperty("exchanges.active", "exchange1,exchange2");
        props.setProperty("polling_period", "30");

        exchanges.put("exchange1", getMockExchange("exchange1"));
        exchanges.put("exchange2", getMockExchange("exchange2"));
        when(providerMock.getExchanges()).thenReturn(exchanges.values().iterator());
    }

    @Test
    public void testLoadingSomeConfiguredExchanges() {
        props.setProperty("exchanges.active", "exchange1");

        // list of expected exchange objects
        List<Exchange> configured = new ArrayList<Exchange>();
        configured.add(exchanges.get("exchange1"));

        Config config = new Config(props);
        assertTrue(configured.containsAll(config.getExchanges()));
    }

    @Test(expected=Error.class)
    public void zeroPollingPeriod() {
        props.setProperty("polling_period", "0");
        Config config = new Config(props);
    }

    @Test(expected=Error.class)
    public void negativePollingPeriod() {
        props.setProperty("polling_period", "-1");
        Config config = new Config(props);
    }

    private Exchange getMockExchange(String name) {
        Exchange exchange = mock(Exchange.class, RETURNS_DEEP_STUBS);
        when(exchange.getExchangeSpecification().getExchangeName()).thenReturn(name);
        return exchange;
    }
}
