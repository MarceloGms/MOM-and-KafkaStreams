package serdes.trip;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Trip;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TripSerializer implements Serializer<Trip> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Trip data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Trip object", e);
        }
    }

    @Override
    public void close() {
    }
}
