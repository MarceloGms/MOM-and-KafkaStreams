package tp3.kafka;

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

import tp3.kafka.serdes.results.ResultsSerde;
import tp3.kafka.serdes.results.ResultsSerializer;
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

                KStream<String, Trip> tripsStream = builder.stream("Trips",
                                Consumed.with(Serdes.String(), new TripSerde()));

                KStream<String, Route> routesStream = builder.stream("Routes",
                                Consumed.with(Serdes.String(), new RouteSerde()));

        // 4 => Get passengers per route
        tripsStream
                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.Long(), new TripSerde()))
                .aggregate(
                        () -> "",
                        (key, value, aggregate) -> aggregate.isEmpty() ? value.getPassengerName() : aggregate + ", " + value.getPassengerName(),
                        Materialized.with(Serdes.Long(), Serdes.String())
                )
                .toStream()
                .map((key, value) -> {
                        String newKey = String.valueOf(key);
                        Results result = new Results(value);
                        return new KeyValue<>(newKey, result);
                    })
                .peek((key, value) -> System.out.println("Passengers for route " + key + ": " + value))
                .to("ResultsPassengersPerRoute", Produced.with(Serdes.String(), new ResultsSerde()));

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
                                .groupBy((key, value) -> value.getRouteId(),
                                                Grouped.with(Serdes.Long(), new TripSerde()))
                                .count(Materialized.with(Serdes.Long(), Serdes.Long())) // Count passengers per route
                                .join(
                                                routesStream
                                                                .groupBy((key, value) -> value.getRouteId(),
                                                                                Grouped.with(Serdes.Long(),
                                                                                                new RouteSerde()))
                                                                .aggregate(
                                                                                () -> 0,
                                                                                (key, value, aggregate) -> aggregate
                                                                                                + value.getCapacity(),
                                                                                Materialized.with(Serdes.Long(),
                                                                                                Serdes.Integer())),
                                                (passengerCount, availableSeats) -> {
                                                        double occupancy = (availableSeats == 0) ? 0
                                                                        : (double) passengerCount / availableSeats
                                                                                        * 100;
                                                        return String.format("%.2f%%", occupancy); // Format as
                                                                                                   // percentage
                                                },
                                                Materialized.with(Serdes.Long(), Serdes.String()))
                                .toStream()
                                .peek((key, value) -> System.out
                                                .println("Occupancy PODRE for route " + key + ": " + value))
                                .to("Results-OccupancyPercentagePerRoute",
                                                Produced.with(Serdes.Long(), Serdes.String()));

                // 7 => Count total number of passengers
                tripsStream
                                .groupBy((key, value) -> "total", Grouped.with(Serdes.String(), new TripSerde()))
                                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                                .toStream()
                                .peek((key, value) -> System.out.println("Total number of passengers: " + value))
                                .to("Results-TotalPassengerCount", Produced.with(Serdes.String(), Serdes.Long()));

                KafkaStreams streams = new KafkaStreams(builder.build(), props);
                streams.start();

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
                Materialized.with(Serdes.String(), new LongArraySerde()) // Usar um tipo customizado para armazenar o total de passageiros e viagens
                )
                .toStream()
                .mapValues((key, value) -> {
                long totalPassengers = value[0]; // Total de passageiros
                long totalTrips = value[1]; // Total de viagens
                double averagePassengers = totalTrips == 0 ? 0 : (double) totalPassengers / totalTrips; // Calcular média
                return String.format("Average passengers per trip: %.2f", averagePassengers); // Formatar média
                })
                .peek((key, value) -> System.out.println("Average passengers for transport type " + key + ": " + value))
                .to("Results-AveragePassengersPerTransportType", Produced.with(Serdes.String(), Serdes.String())); // Enviar o resultado para o tópico Kafka



                Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }
}
