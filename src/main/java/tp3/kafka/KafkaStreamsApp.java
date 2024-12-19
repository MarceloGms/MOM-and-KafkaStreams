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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import org.apache.kafka.streams.kstream.TimeWindows;


import java.time.Duration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.StdArraySerializers.IntArraySerializer;

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

                ObjectMapper objectMapper = new ObjectMapper();

                KStream<String, Trip> tripsStream = builder.stream("Trips", Consumed.with(Serdes.String(), new TripSerde()));

                KStream<Long, Route> routesStream = builder.stream("Routes", Consumed.with(Serdes.String(), new RouteSerde()))
                        .map((key, value) -> KeyValue.pair(value.getRouteId(), value));

                // 4 => Get passengers per route
                tripsStream
                        .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.Long(), new TripSerde()))
                        .aggregate(
                                () -> "",
                                (key, value, aggregate) ->
                                aggregate.isEmpty() ? value.getPassengerName() : aggregate + ", " + value.getPassengerName(),
                                Materialized.with(Serdes.Long(), Serdes.String())
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Passengers for route " + key + ": " + value))
                        .mapValues((routeId, passengerList) -> createResult(routeId, passengerList, "ResultsPassengersPerRoute"))
                        .to("ResultsPassengersPerRoute", Produced.with(Serdes.Long(), new ResultsSerde()));

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
                        .mapValues((routeId, availableSeats) -> createResult(routeId, String.valueOf(availableSeats), "ResultsAvailableSeatsPerRoute"))
                        .to("ResultsAvailableSeatsPerRoute", Produced.with(Serdes.Long(), new ResultsSerde()));

                // 6 => Occupancy percentage per route
                tripsStream
                        .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.Long(), new TripSerde()))
                        .count(Materialized.with(Serdes.Long(), Serdes.Long()))
                        .join(routesStream
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
                        .mapValues((routeId, occupancy) -> createResult(routeId, occupancy, "ResultsOccupancyPercentagePerRoute"))
                        .to("ResultsOccupancyPercentagePerRoute", Produced.with(Serdes.Long(), new ResultsSerde()));


                // 7 => Count total number of passengers
                tripsStream
                        .groupBy((key, value) -> "total", Grouped.with(Serdes.String(), new TripSerde()))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream()
                        .peek((key, value) -> System.out.println("Total number of passengers: " + value))
                        .mapValues((key, value) -> createResult(key, String.valueOf(value), "ResultsTotalPassengerCount"))
                        .to("ResultsTotalPassengerCount", Produced.with(Serdes.String(), new ResultsSerde()));

                // 8 => Get total seating available for all routes
                routesStream
                        .groupBy((key, value) -> "total", Grouped.with(Serdes.String(), new RouteSerde())) // Group by a common key "total"
                        .aggregate(
                                () -> 0,
                                (key, value, aggregate) -> aggregate + value.getCapacity(), // Sum total available seats
                                Materialized.with(Serdes.String(), Serdes.Integer()) // Define the state store type for the aggregation
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Total seating available for all routes: " + value)) // Log the result
                        .mapValues((key, value) -> createResult(key, String.valueOf(value), "ResultsTotalSeatingAvailable")) // Create a Results object
                        .to("ResultsTotalSeatingAvailable", Produced.with(Serdes.String(), new ResultsSerde())); // Publish to Kafka topic

        
                // 9 => Get the occupancy percentage total (for all routes)
                tripsStream
                        .map((key, value) -> KeyValue.pair("total", 1L)) // Map each trip to a common key "total"
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())) // Group by the common key
                        .reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long())) // Sum total passengers
                        .join(routesStream
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
                        .mapValues((key, value) -> createResult(key, value, "ResultsTotalOccupancyPercentage")) // Create a Results object
                        .to("ResultsTotalOccupancyPercentage", Produced.with(Serdes.String(), new ResultsSerde())); // Publish to Kafka topic

        
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
                                return String.format("%.2f", averagePassengers); // Formatar média
                        })
                        .peek((key, value) -> System.out.println("Average passengers for transport type " + key + ": " + value))
                        .mapValues((key, value) -> createResult(key, value, "ResultsAveragePassengersPerTransportType"))
                        .to("ResultsAveragePassengersPerTransportType", Produced.with(Serdes.String(), new ResultsSerde()));



                // 11 => Get the transport type with the highest number of served passengers (only one if there is a tie)
                //! ver pq dois groupBy
                tripsStream
                        .groupBy((key, value) -> value.getTransportType(), Grouped.with(Serdes.String(), new TripSerde())) // Group by transport type
                        .count(Materialized.with(Serdes.String(), Serdes.Long())) // Count the number of passengers for each transport type
                        .toStream()
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())) // Group by key (transport type)
                        .reduce(
                                (value1, value2) -> value1 >= value2 ? value1 : value2 // Use ternary operator with tie breaker
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Transport type with the highest passengers: " + key + "-" + value))
                        .mapValues((key, value) -> createResult("highest", "" + key + " -> " + value, "ResultsHighestTransportType"))
                        .to("ResultsHighestTransportType", Produced.with(Serdes.String(), new ResultsSerde()));

                // 12 => Get the routes with the least occupancy per transport type
                // Aggregate passengers per route

                KTable<String, Route> routeTable = routesStream
                .selectKey((key, value) -> key.toString())
                .toTable(Materialized.with(Serdes.String(), new RouteSerde()));

                KStream<String, String> occupancyStream = tripsStream.join(
                        routeTable,
                        (trip, route) -> {
                                int tripCount = 1; 
                                double occupancy = (double) tripCount / route.getCapacity();
                                return "RouteId: " + route.getRouteId() + " Type: " + route.getType() + " Occupancy: " + occupancy;
                        }
                );

                // Aggregate occupancy by transport type
                KTable<String, String> occupancyByTypeTable = occupancyStream
                .groupBy((key, value) -> value.split(" Type: ")[1].split(" ")[0], Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        // Initialize with an empty string, which will store the routeId and occupancy
                        () -> "",
                        // Compare and choose the route with the least occupancy
                        (key, value, aggregate) -> {
                                // Extract the routeId and occupancy from the value string
                                double currentOccupancy = Double.parseDouble(value.split(" Occupancy: ")[1]);
                                
                                // If the aggregate is empty, initialize it with the current value
                                if (aggregate.isEmpty()) {
                                        return value;  // Return the current route with routeId and occupancy
                                }

                                // Extract the aggregate occupancy and routeId
                                double aggregateOccupancy = Double.parseDouble(aggregate.split(" Occupancy: ")[1]);

                                // Choose the route with the least occupancy
                                if (currentOccupancy < aggregateOccupancy) {
                                        return value;  // Current route has lower occupancy, so update the aggregate
                                } else {
                                        return aggregate;  // Existing route still has the least occupancy
                                }
                        }
                )
                .mapValues(value -> {
                        // Extract routeId and occupancy for the final result format
                        String routeId = value.split(" RouteId: ")[1].split(" ")[0];
                        double occupancy = Double.parseDouble(value.split(" Occupancy: ")[1]);
                        // Return formatted result
                        return routeId + " -> " + occupancy;
                });

                occupancyByTypeTable
                        .toStream()
                        .peek((key, value) -> System.out.println("Route with the least occupancy for transport type " + key + ": " + value))
                        .mapValues((key, value) -> createResult(key, value, "ResultsLeastOccupiedTransportType"))
                        .to("ResultsLeastOccupiedTransportType", Produced.with(Serdes.String(), new ResultsSerde()));
                        
                // 13 => Get the most used transport type in the last hour using a tumbling window
                tripsStream
                        .groupBy((key, value) -> value.getTransportType(), Grouped.with(Serdes.String(), new TripSerde()))
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream()
                        .map((key, value) -> KeyValue.pair(key.key(), value))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                        .reduce(
                                (value1, value2) -> value1 > value2 ? value1 : value2,
                                Materialized.with(Serdes.String(), Serdes.Long())
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Most used transport type in the last hour: " + key + " with " + value + " passengers"))
                        .mapValues((key, value) -> createResult("mostUsed", "" + key + " -> " + value, "ResultsMostUsedTransportType"))
                        .to("ResultsMostUsedTransportType", Produced.with(Serdes.String(), new ResultsSerde()));

                /* // 14 => Get the least occupied transport type in the last hour using a tumbling window

// Join tripsStream with routesStream to get transportType and capacity
tripsStream
.groupBy((key, value) -> value.getTransportType(), Grouped.with(Serdes.String(), Serdes.serdeFrom(Trip.class))) // Group by transport type
.aggregate(
    () -> new int[]{0, 0}, // [passengerCount, capacity]
    (key, value, aggregate) -> {
        aggregate[0] += 1; // Increment passenger count
        // You can assume that each transportType corresponds to a constant capacity
        // For simplicity, we'll join with routesStream later to get actual capacities.
        return aggregate;
    }
)
.toStream()
.join(
    routesStream
        .groupBy((key, value) -> value.getType(), Grouped.with(Serdes.String(), Serdes.serdeFrom(Route.class))) // Group by transportType
        .aggregate(
            () -> 0, // Initial capacity for each transportType
            (key, value, aggregate) -> aggregate + value.getCapacity(), // Aggregate capacity for each transportType
            Materialized.with(Serdes.String(), Serdes.Integer()) // Serializer for Integer (capacity)
        )
        .toStream(),
    (occupancy, capacity) -> {
        // Calculate occupancy percentage
        double occupancyRate = (capacity == 0) ? 0 : (double) occupancy[0] / capacity * 100;
        return occupancyRate;
    },
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1)) // Ensure we join within the window
)
.groupByKey(Grouped.with(Serdes.String(), Serdes.Double())) // Group by transportType and occupancy rate
.reduce(
    (rate1, rate2) -> rate1 < rate2 ? rate1 : rate2, // Find the least occupied transport type
    Materialized.with(Serdes.String(), Serdes.Double()) // Store the result as occupancy rate
)
.toStream()
.peek((key, value) -> System.out.println("Least occupied transport type in the last hour: " + key + " with occupancy rate: " + value))
.mapValues((key, value) -> createResult("leastOccupied", key + " -> " + value, "ResultsLeastOccupiedTransportType"))
.to("ResultsLeastOccupiedTransportType", Produced.with(Serdes.String(), new ResultsSerde())); */




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

        private static Results createResult(String id, String value, String schemaName) {
                return new Results(
                        getSchemaDefinition(schemaName),
                        Map.of(
                        "id", id,
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
