package tp3.kafka.serdes.route;

import com.fasterxml.jackson.databind.ObjectMapper;

import tp3.persistence.entity.Route;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class RouteDeserializer implements Deserializer<Route> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Route deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Route.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing Route object", e);
        }
    }

    @Override
    public void close() {
    }
}