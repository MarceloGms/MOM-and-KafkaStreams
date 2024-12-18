package tp3.kafka.serdes.results;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import tp3.persistence.entity.Results;

import java.util.Map;

public class ResultsSerde extends Serdes.WrapperSerde<Results> {

    public ResultsSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(Results.class));
    }

    // JsonSerializer
    public static class JsonSerializer<T> implements Serializer<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, T data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize object", e);
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public void close() {}
    }

    // JsonDeserializer
    public static class JsonDeserializer<T> implements Deserializer<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final Class<T> targetType;

        public JsonDeserializer(Class<T> targetType) {
            this.targetType = targetType;
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, targetType);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize object", e);
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public void close() {}
    }
}
