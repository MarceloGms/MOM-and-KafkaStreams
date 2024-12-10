import entity.Route;
import entity.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import serdes.route.RouteSerde;
import serdes.trip.TripSerde;

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
                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.String(), new TripSerde()))
                .aggregate(
                        () -> "",
                        (key, value, aggregate) -> aggregate.isEmpty() ? value.getPassengerName() : aggregate + ", " + value.getPassengerName(),
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .peek((key, value) -> System.out.println("Passengers for route " + key + ": " + value))
                .to("Results-PassengersPerRoute", Produced.with(Serdes.String(), Serdes.String()));

        // 5 => Get available seats per route
        routesStream
                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.String(), new RouteSerde()))
                .aggregate(
                        () -> 0,
                        (key, value, aggregate) -> aggregate + value.getCapacity(),
                        Materialized.with(Serdes.String(), Serdes.Integer())
                )
                .toStream()
                .peek((key, value) -> System.out.println("Available seats for route " + key + ": " + value))
                .to("Results-AvailableSeatsPerRoute", Produced.with(Serdes.String(), Serdes.Integer()));

        // 7 => Get total passengers
        //! ns se total passengers para cada rota ou total de passageiros no geral?
        tripsStream
                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.String(), new TripSerde()))
                .count()
                .toStream()
                .peek((key, value) -> System.out.println("Total passengers for route " + key + ": " + value))
                .to("Results-TotalPassengersPerRoute", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
