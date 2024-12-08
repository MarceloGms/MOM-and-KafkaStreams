package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Route;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;

public class RouteSerde implements Serde<Route> {

    private final Serde<Route> inner;

    public RouteSerde() {
        inner = Serdes.serdeFrom(new RouteSerializer(), new RouteDeserializer());
    }

    @Override
    public Serializer<Route> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<Route> deserializer() {
        return inner.deserializer();
    }

    public static class RouteSerializer implements Serializer<Route> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, Route data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing Route", e);
            }
        }

        @Override
        public void close() {
        }
    }

    public static class RouteDeserializer implements Deserializer<Route> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public Route deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, Route.class);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing Route", e);
            }
        }

        @Override
        public void close() {
        }
    }
}
