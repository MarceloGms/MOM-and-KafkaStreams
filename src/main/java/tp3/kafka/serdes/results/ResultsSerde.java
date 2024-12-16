package tp3.kafka.serdes.results;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ResultsSerde implements Serde<String> {

    private final ResultsSerializer serializer = new ResultsSerializer();

    @Override
    public Serializer<String> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<String> deserializer() {
        return null;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
    }
}
