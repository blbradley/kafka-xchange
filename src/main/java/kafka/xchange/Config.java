package kafka.xchange;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
    private final Logger logger = LoggerFactory.getLogger(Config.class);
    private int pollingPeriod;

    Config(Properties props) {
        pollingPeriod = Integer.parseInt(props.getProperty("polling_period"));
        if (pollingPeriod <= 0) {
            throw new Error("polling_period should be greater than zero.");
        }
    }

    public int getPollingPeriod() {
        return pollingPeriod;
    }
}
