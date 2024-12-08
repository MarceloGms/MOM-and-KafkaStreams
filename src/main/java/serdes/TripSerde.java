package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Trip;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;

public class TripSerde implements Serde<Trip> {

    private final Serde<Trip> inner;

    public TripSerde() {
        inner = Serdes.serdeFrom(new TripSerializer(), new TripDeserializer());
    }

    @Override
    public Serializer<Trip> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<Trip> deserializer() {
        return inner.deserializer();
    }

    public static class TripSerializer implements Serializer<Trip> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, Trip data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing Trip", e);
            }
        }

        @Override
        public void close() {
        }
    }

    public static class TripDeserializer implements Deserializer<Trip> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public Trip deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, Trip.class);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing Trip", e);
            }
        }

        @Override
        public void close() {
        }
    }
}
