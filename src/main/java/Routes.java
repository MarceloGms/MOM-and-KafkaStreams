   // Stream<k, v> -> Stream<String/long, object>
   // Ter cenario de teste bem montado, producer sem randoms
   // no broker crash fazer um cenario do tipo 0, 1 ou all

import com.fasterxml.jackson.databind.ObjectMapper;

import entity.Route;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Routes {

   private static final String TOPIC = "Routes";
   private static final String BROKER = "broker1:9092";
   private static final Random RANDOM = new Random();
   private static final String[] TRANSPORT_TYPES = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
   private static final String[] OPERATORS = {"OperatorA", "OperatorB", "OperatorC"};

   public static void main(String[] args) {
      Properties properties = new Properties();
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      Producer<String, String> producer = new KafkaProducer<>(properties);
      ObjectMapper objectMapper = new ObjectMapper();

      for (int i = 0; i < 100; i++) {
            String routeId = "Route" + RANDOM.nextInt(10);
            String origin = "City" + RANDOM.nextInt(10);
            String destination = "City" + RANDOM.nextInt(10);
            String transportType = TRANSPORT_TYPES[RANDOM.nextInt(TRANSPORT_TYPES.length)];
            String operator = OPERATORS[RANDOM.nextInt(OPERATORS.length)];
            int capacity = RANDOM.nextInt(200) + 1;

            Route route = new Route(routeId, origin, destination, transportType, operator, capacity);

            try {
               String routeJson = objectMapper.writeValueAsString(route);
               producer.send(new ProducerRecord<>(TOPIC, origin + "-" + destination, routeJson));
               System.out.println("Sent route: " + routeJson);
            } catch (Exception e) {
               e.printStackTrace();
            }
      }

      producer.close();
   }
}