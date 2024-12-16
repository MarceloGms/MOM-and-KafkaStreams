package tp3.kafka.serdes;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LongArraySerde extends Serdes.WrapperSerde<long[]> {
    public LongArraySerde() {
        super(new LongArraySerializer(), new LongArrayDeserializer());
    }
}

class LongArraySerializer implements org.apache.kafka.common.serialization.Serializer<long[]> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, long[] data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing long[]", e);
        }
    }
}

class LongArrayDeserializer implements org.apache.kafka.common.serialization.Deserializer<long[]> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public long[] deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, long[].class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing long[]", e);
        }
    }
}
