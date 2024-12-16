package tp3.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

import tp3.persistence.entity.Trip;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class PassengerTrips {

    private static final String TOPIC = "Trips";
    private static final String BROKER = "broker1:9092";
    private static final Random RANDOM = new Random();
    private static final String[] TRANSPORT_TYPES = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
    private static final String[] PASSENGER_NAMES = {"Alice", "Bob", "Charlie", "David", "Eve"};

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);
        ObjectMapper objectMapper = new ObjectMapper();

        for (int i = 0; i < 10; i++) {
            long routeId = RANDOM.nextInt(5);
            String passengerName = PASSENGER_NAMES[RANDOM.nextInt(PASSENGER_NAMES.length)];
            String origin = "City" + RANDOM.nextInt(10);
            String destination = "City" + RANDOM.nextInt(10);
            String transportType = TRANSPORT_TYPES[RANDOM.nextInt(TRANSPORT_TYPES.length)];

            Trip trip = new Trip(routeId, passengerName, origin, destination, transportType);

            try {
                String tripJson = objectMapper.writeValueAsString(trip);
                producer.send(new ProducerRecord<>(TOPIC, String.valueOf(routeId), tripJson));
                System.out.println("Sent trip: " + tripJson);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}
