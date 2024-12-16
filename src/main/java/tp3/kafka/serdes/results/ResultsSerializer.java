package tp3.kafka.serdes.results;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class ResultsSerializer implements Serializer<String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, String data) {
        try {
            // Define the schema structure
            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", new Object[]{
                Map.of("type", "string", "optional", false, "field", "results")
            });
            schema.put("optional", false);
            schema.put("name", "results schema");

            // Define the payload structure
            Map<String, Object> payload = new HashMap<>();
            payload.put("results", data);

            // Combine schema and payload
            Map<String, Object> result = new HashMap<>();
            result.put("schema", schema);
            result.put("payload", payload);

            // Serialize to JSON
            return objectMapper.writeValueAsBytes(result);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing results", e);
        }
    }
}
