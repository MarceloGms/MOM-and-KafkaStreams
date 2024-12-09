package serdes.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Route;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class RouteSerializer implements Serializer<Route> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Route data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Route object", e);
        }
    }

    @Override
    public void close() {
    }
}