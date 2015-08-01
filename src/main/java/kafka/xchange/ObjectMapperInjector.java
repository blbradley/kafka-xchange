package kafka.xchange;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface ObjectMapperInjector {
    static ObjectMapper mapper = new ObjectMapper();
}
