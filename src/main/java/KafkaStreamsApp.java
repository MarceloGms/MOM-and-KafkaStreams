import entity.Route;
import entity.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import serdes.RouteSerde;
import serdes.TripSerde;

import java.util.Properties;

public class KafkaStreamsApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "routes-trips-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Consume the Routes topic
        KTable<String, Route> routesTable = builder.table("Routes", Consumed.with(Serdes.String(), new RouteSerde()));

        // Consume the Trips topic
        KStream<String, Trip> tripsStream = builder.stream("Trips", Consumed.with(Serdes.String(), new TripSerde()));

        // Join the Trips stream with the Routes table
        KStream<String, Trip> enrichedTripsStream = tripsStream.join(routesTable,
                (trip, route) -> {
                    trip.setOrigin(route.getOrigin());
                    trip.setDestination(route.getDestination());
                    trip.setTransportType(route.getTransportType());
                    return trip;
                });

        // Print the enriched trips
        enrichedTripsStream.foreach((key, trip) -> System.out.println("Enriched trip: " + trip));

        // Write the enriched trips to a new topic
        enrichedTripsStream.to("EnrichedTrips", Produced.with(Serdes.String(), new TripSerde()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
