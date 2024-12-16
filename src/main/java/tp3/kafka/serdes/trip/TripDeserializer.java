package tp3.kafka.serdes.trip;

import com.fasterxml.jackson.databind.ObjectMapper;

import tp3.persistence.entity.Trip;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class TripDeserializer implements Deserializer<Trip> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Trip deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Trip.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing Trip object", e);
        }
    }

    @Override
    public void close() {
    }
}
