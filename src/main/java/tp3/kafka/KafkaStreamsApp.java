package tp3.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import com.fasterxml.jackson.databind.ObjectMapper;

import tp3.kafka.serdes.results.ResultsSerde;
import tp3.kafka.serdes.route.RouteSerde;
import tp3.kafka.serdes.trip.TripSerde;
import tp3.persistence.entity.Results;
import tp3.persistence.entity.Route;
import tp3.persistence.entity.Trip;

public class KafkaStreamsApp {

        public static void main(String[] args) {
                Properties props = new Properties();
                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "routes-trips-processor");
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

                StreamsBuilder builder = new StreamsBuilder();

                KStream<String, Trip> tripsStream = builder.stream("Trips", Consumed.with(Serdes.String(), new TripSerde()));

                KStream<String, Route> routesStream = builder.stream("Routes", Consumed.with(Serdes.String(), new RouteSerde()));

                ObjectMapper objectMapper = new ObjectMapper();

                // 4 => Get passengers per route
                tripsStream
                        .groupBy(
                                (key, value) -> value.getRouteId(),
                                Grouped.with(Serdes.Long(), new TripSerde())
                        )
                        .aggregate(
                                () -> "",
                                (key, value, aggregate) -> aggregate.isEmpty()
                                ? value.getPassengerName()
                                : aggregate + ", " + value.getPassengerName(),
                                Materialized.with(Serdes.Long(), Serdes.String())
                        )
                        .toStream()
                        .mapValues((routeId, passengerList) -> 
                                createResult(routeId, passengerList, "ResultsPassengersPerRoute")
                        )
                        .peek((key, value) -> 
                                System.out.println("Passengers for route " + key + ": " + value)
                        )
                        .to("ResultsPassengersPerRoute", Produced.with(Serdes.Long(), new ResultsSerde()));


                // 5 => Get available seats per route
                routesStream
                        .groupBy((key, value) -> value.getRouteId(),
                                        Grouped.with(Serdes.Long(), new RouteSerde()))
                        .aggregate(
                                        () -> 0,
                                        (key, value, aggregate) -> aggregate + value.getCapacity(),
                                        Materialized.with(Serdes.Long(), Serdes.Integer()))
                        .toStream()
                        .peek((key, value) -> System.out
                                        .println("Available seats for route " + key + ": " + value))
                        .to("Results-AvailableSeatsPerRoute", Produced.with(Serdes.Long(), Serdes.Integer()));

                // 6 => Occupancy percentage per route
                tripsStream
                        .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.Long(), new TripSerde()))
                        .count(Materialized.with(Serdes.Long(), Serdes.Long()))
                        .join(
                        routesStream
                                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.Long(), new RouteSerde()))
                                .aggregate(
                                        () -> 0,
                                        (key, value, aggregate) -> aggregate + value.getCapacity(),
                                        Materialized.with(Serdes.Long(), Serdes.Integer())
                                ),
                        (passengerCount, availableSeats) -> {
                                double occupancy = (availableSeats == 0) ? 0 : (double) passengerCount / availableSeats * 100;
                                return String.format("%.2f%%", occupancy);
                        },
                        Materialized.with(Serdes.Long(), Serdes.String())
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Occupancy for route " + key + ": " + value))
                        .to("Results-OccupancyPercentagePerRoute", Produced.with(Serdes.Long(), Serdes.String()));


                // 7 => Count total number of passengers
                tripsStream
                        .groupBy((key, value) -> "total", Grouped.with(Serdes.String(), new TripSerde()))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream()
                        .peek((key, value) -> System.out.println("Total number of passengers: " + value))
                        .to("Results-TotalPassengerCount", Produced.with(Serdes.String(), Serdes.Long()));

                // 8 => Get total seating available for all routes
                routesStream
                        .map((key, value) -> KeyValue.pair("total", value.getCapacity()))  // Map para uma chave comum "total" para todas as rotas
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))  // Agrupar todas as entradas pela chave "total"
                        .reduce(
                        Integer::sum,  // Somar os assentos disponíveis para todas as rotas
                        Materialized.with(Serdes.String(), Serdes.Integer())  // Definir o tipo de store de estado para a agregação
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Total seating available for all routes: " + value))  // Exibir o resultado
                        .to("Results-TotalSeatingAvailable", Produced.with(Serdes.String(), Serdes.Integer()));  // Enviar o resultado para um tópico Kafka

        
                // 9 => Get the occupancy percentage total (for all routes)
                tripsStream
                        .map((key, value) -> KeyValue.pair("total", 1L)) // Map each trip to a common key "total"
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())) // Group by the common key
                        .reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long())) // Sum total passengers
                        .join(
                                routesStream
                                        .map((key, value) -> KeyValue.pair("total", value.getCapacity())) // Map each route to a common key "total"
                                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer())) // Group by the common key
                                        .reduce(Integer::sum, Materialized.with(Serdes.String(), Serdes.Integer())), // Sum total available seats
                                (totalPassengers, totalSeats) -> {
                                        double occupancy = (totalSeats == 0) ? 0 : (double) totalPassengers / totalSeats * 100; // Calculate total occupancy
                                        return String.format("%.2f%%", occupancy); // Format as percentage
                                },
                                Materialized.with(Serdes.String(), Serdes.String()) // Materialize the store for the result
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Total occupancy percentage: " + value)) // Log the result
                        .to("Results-TotalOccupancyPercentage", Produced.with(Serdes.String(), Serdes.String())); // Publish to Kafka topic

        
                // 10 => Get the average number of passengers per transport type
                tripsStream
                        .groupBy((key, value) -> value.getTransportType(), Grouped.with(Serdes.String(), new TripSerde())) // Agrupar por tipo de transporte
                        .aggregate(
                                () -> new long[]{0, 0}, // Array [0] -> total de passageiros, [1] -> número de viagens
                                (key, value, aggregate) -> {
                                aggregate[0] += 1;  // Incrementa a contagem de passageiros
                                aggregate[1] += 1;  // Incrementa o número de viagens
                                return aggregate;
                                },
                                Materialized.with(
                                Serdes.String(),
                                Serdes.serdeFrom(
                                        (topic, data) -> { // Serializer
                                        try {
                                                return objectMapper.writeValueAsBytes(data);
                                        } catch (Exception e) {
                                                throw new RuntimeException("Error serializing long array", e);
                                        }
                                        },
                                        (topic, data) -> { // Deserializer
                                        try {
                                                return objectMapper.readValue(data, long[].class);
                                        } catch (Exception e) {
                                                throw new RuntimeException("Error deserializing long array", e);
                                        }
                                        }
                                )
                                )
                        )
                        .toStream()
                        .mapValues((key, value) -> {
                                long totalPassengers = value[0]; // Total de passageiros
                                long totalTrips = value[1]; // Total de viagens
                                double averagePassengers = totalTrips == 0 ? 0 : (double) totalPassengers / totalTrips; // Calcular média
                                return String.format("Average passengers per trip: %.2f", averagePassengers); // Formatar média
                        })
                        .peek((key, value) -> System.out.println("Average passengers for transport type " + key + ": " + value))
                        .to("Results-AveragePassengersPerTransportType", Produced.with(Serdes.String(), Serdes.String()));


                // 11 => Get the transport type with the highest number of served passengers (only one if there is a tie)
                tripsStream
                .groupBy((key, value) -> value.getTransportType(), Grouped.with(Serdes.String(), new TripSerde())) // Group by transport type
                .count(Materialized.with(Serdes.String(), Serdes.Long())) // Count the number of passengers for each transport type
                .toStream()
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())) // Group by key (transport type)
                .reduce(
                        (value1, value2) -> {
                        // If value1 has more passengers than value2, keep value1
                        if (value1 > value2) {
                                return value1;
                        }
                        // If value2 has more passengers, keep value2
                        else if (value1 < value2) {
                                return value2;
                        }
                        // If both have the same count, keep the lexicographically smaller key (transport type)
                        else {
                                return value1.compareTo(value2) < 0 ? value1 : value2;
                        }
                        }
                )
                .toStream()
                .map((key, value) -> {
                        // Map to a string representing the highest transport type and its count
                        return KeyValue.pair("HighestTransportType", key + ": " + value);
                })
                .peek((key, value) -> System.out.println("Transport type with the highest passengers: " + value)) // Log the result
                .to("Results-HighestTransportType", Produced.with(Serdes.String(), Serdes.String())); // Publish to Kafka topic

            

                KafkaStreams streams = new KafkaStreams(builder.build(), props);
                streams.start();
                Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }

        private static Results createResult(Long id, String value, String schemaName) {
                return new Results(
                        getSchemaDefinition(schemaName),
                        Map.of(
                        "id", String.valueOf(id),
                        "result", value
                        )
                );
        }

        private static Map<String, Object> getSchemaDefinition(String name) {
                return Map.of(
                    "type", "struct",
                    "fields", List.of(
                        Map.of("field", "id", "type", "string"),
                        Map.of("field", "result", "type", "string")
                    ),
                    "optional", false,
                    "name", name
                );
        }
}
