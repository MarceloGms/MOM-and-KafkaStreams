import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class FraudDetectionApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper objectMapper = new ObjectMapper();

        // Read transactions from input topic
        KStream<String, String> transactions = builder.stream("transactions");

        KStream<String, String> suspiciousTransactions = transactions
            // Deserialize JSON transaction
            .mapValues(value -> {
                try {
                    return objectMapper.readValue(value, Transaction.class);
                } catch (Exception e) {
                    return null;
                }
            })
            .filter((key, transaction) -> transaction != null) // Remove invalid transactions
            // Detect suspicious transactions
            .filter((key, transaction) -> 
            transaction.getTransactionAmount() > 10_000 || // Rule: High-value transaction
            transaction.getTransactionAmount() > 5_000 && 
            (System.currentTimeMillis() - transaction.getTimestamp()) < 60_000 // Rule: Frequent transactions
            )
            // Serialize the suspicious transactions back to JSON
            .mapValues(transaction -> {
                try {
                    return objectMapper.writeValueAsString(transaction);
                } catch (Exception e) {
                    return null;
                }
            })
            .filter((key, value) -> value != null); // Remove null values if serialization fails

        // Write suspicious transactions to the output topic
        suspiciousTransactions.to("suspicious-transactions", Produced.with(Serdes.String(), Serdes.String()));

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

