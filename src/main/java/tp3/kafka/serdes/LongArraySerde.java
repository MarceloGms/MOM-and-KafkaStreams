package tp3.kafka.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LongArraySerde extends Serdes.WrapperSerde<long[]> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public LongArraySerde() {
        super(new Serializer<long[]>() {
            @Override
            public byte[] serialize(String topic, long[] data) {
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException("Error serializing long array", e);
                }
            }
        }, new Deserializer<long[]>() {
            @Override
            public long[] deserialize(String topic, byte[] data) {
                try {
                    return objectMapper.readValue(data, long[].class);
                } catch (Exception e) {
                    throw new RuntimeException("Error deserializing long array", e);
                }
            }
        });
    }
}
