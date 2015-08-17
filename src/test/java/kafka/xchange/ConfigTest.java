package kafka.xchange;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.Before;

import kafka.xchange.Config;

public class ConfigTest {
    Properties props = new Properties();
    
    @Before
    public void setUp() {
        props.setProperty("exchanges.active", "exchange1,exchange2");
        props.setProperty("polling_period", "30");
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
}
