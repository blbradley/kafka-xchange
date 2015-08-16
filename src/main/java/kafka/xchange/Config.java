package kafka.xchange;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
    private final Logger logger = LoggerFactory.getLogger(Config.class);
    private int pollingPeriod;

    Config(Properties props) {
        pollingPeriod = Integer.parseInt(props.getProperty("polling_period"));
    }

    public int getPollingPeriod() {
        return pollingPeriod;
    }
}
