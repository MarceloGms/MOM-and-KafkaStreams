package tp3.kafka;

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

import java.util.Properties;

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
                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.Long(), new RouteSerde()))
                .aggregate(
                        () -> 0,
                        (key, value, aggregate) -> aggregate + value.getCapacity(),
                        Materialized.with(Serdes.Long(), Serdes.Integer())
                )
                .toStream()
                .peek((key, value) -> System.out.println("Available seats for route " + key + ": " + value))
                .to("ResultsAvailableSeatsPerRoute", Produced.with(Serdes.Long(), Serdes.Integer()));

        // 7 => Count total number of passengers
        tripsStream
                .groupBy((key, value) -> "total", Grouped.with(Serdes.String(), new TripSerde()))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .peek((key, value) -> System.out.println("Total number of passengers: " + value))
                .to("ResultsTotalPassengerCount", Produced.with(Serdes.String(), Serdes.Long()));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
