package tp3.kafka.serdes.results;

import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

import tp3.persistence.entity.Results;

public class ResultsSerializer implements Serializer<Results> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuration logic if needed
    }

    @Override
    public byte[] serialize(String topic, Results data) {
        try {
            if (data == null) {
                return null;
            }
            // Convert Results object to JSON byte array
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Results object", e);
        }
    }

    @Override
    public void close() {
        // Cleanup if needed
    }
}
