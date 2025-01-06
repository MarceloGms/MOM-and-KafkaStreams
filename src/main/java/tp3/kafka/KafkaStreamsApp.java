package tp3.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import java.time.Duration;

import tp3.kafka.serdes.LongArraySerde;
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
                        .count(Materialized.with(Serdes.Long(), Serdes.Long())) // Count number of passengers per route
                        .join(routesStream
                                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.Long(), new RouteSerde()))
                                .aggregate( // Aggregate total capacity per route
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
                        .groupBy((key, value) -> "total", Grouped.with(Serdes.String(), new RouteSerde()))
                        .aggregate(
                                () -> 0,
                                (key, value, aggregate) -> aggregate + value.getCapacity(),
                                Materialized.with(Serdes.String(), Serdes.Integer())
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Total seating available for all routes: " + value))
                        .mapValues((key, value) -> createResult(key, String.valueOf(value), "ResultsTotalSeatingAvailable"))
                        .to("ResultsTotalSeatingAvailable", Produced.with(Serdes.String(), new ResultsSerde()));

        
                // 9 => Get the occupancy percentage total (for all routes)
                tripsStream
                        .groupBy((key, value) -> "total", Grouped.with(Serdes.String(), new TripSerde()))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .join(routesStream
                                .map((key, value) -> KeyValue.pair("total", value.getCapacity()))
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                                .reduce(Integer::sum, Materialized.with(Serdes.String(), Serdes.Integer())),
                                (totalPassengers, totalSeats) -> {
                                        double occupancy = (totalSeats == 0) ? 0 : (double) totalPassengers / totalSeats * 100;
                                        return String.format("%.2f%%", occupancy);
                                },
                                Materialized.with(Serdes.String(), Serdes.String())
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Total occupancy percentage: " + value))
                        .mapValues((key, value) -> createResult(key, value, "ResultsTotalOccupancyPercentage"))
                        .to("ResultsTotalOccupancyPercentage", Produced.with(Serdes.String(), new ResultsSerde()));

        
                // 10 => Get the average number of passengers per transport type
                tripsStream
                        .groupBy((key, value) -> value.getTransportType(), Grouped.with(Serdes.String(), new TripSerde()))
                        .aggregate(
                                () -> new long[]{0, 0}, // Array [0] -> total de passageiros, [1] -> número de viagens
                                (key, value, aggregate) -> {
                                aggregate[0] += 1;
                                aggregate[1] += 1;
                                return aggregate;
                                },
                                Materialized.with(Serdes.String(), new LongArraySerde())
                        )
                        .toStream()
                        .mapValues((key, value) -> {
                                long totalPassengers = value[0];
                                long totalTrips = value[1];
                                double averagePassengers = totalTrips == 0 ? 0 : (double) totalPassengers / totalTrips;
                                return String.format("%.2f", averagePassengers);
                        })
                        .peek((key, value) -> System.out.println("Average passengers for transport type " + key + ": " + value))
                        .mapValues((key, value) -> createResult(key, value, "ResultsAveragePassengersPerTransportType"))
                        .to("ResultsAveragePassengersPerTransportType", Produced.with(Serdes.String(), new ResultsSerde()));

                // 11 => Get the transport type with the highest number of served passengers (only one if there is a tie)
                tripsStream
                        .groupBy((key, value) -> value.getTransportType(), Grouped.with(Serdes.String(), new TripSerde()))
                        .count(Materialized.with(Serdes.String(), Serdes.Long())) // Count number of passengers per transport type
                        .toStream()
                        .map((key, value) -> KeyValue.pair("highest", key + "->" + value))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .reduce(
                                (value1, value2) -> {
                                long count1 = Long.parseLong(value1.split("->")[1]);
                                long count2 = Long.parseLong(value2.split("->")[1]);
                                return count1 >= count2 ? value1 : value2;
                                },
                                Materialized.with(Serdes.String(), Serdes.String())
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Transport type with the highest passengers: " + value))
                        .mapValues((key, value) -> createResult("highest", value, "ResultsHighestTransportType"))
                        .to("ResultsHighestTransportType", Produced.with(Serdes.String(), new ResultsSerde()));

            // 12 => Get the routes with the least occupancy per transport type
                KTable<Long, Route> routeTable = routesStream
                .selectKey((key, value) -> Long.parseLong(key)) // Convert keys to Long
                .toTable(Materialized.with(Serdes.Long(), new RouteSerde()));

                // Group trips by route and count the number of trips (passengers) per route
                KTable<Long, Long> tripCounts = tripsStream
                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.Long(), new TripSerde()))
                .count(Materialized.with(Serdes.Long(), Serdes.Long()));

                // Join the aggregated trip counts with the route table to calculate the occupancy
                KStream<Long, String> occupancyStream = tripCounts.toStream().join(
                routeTable,
                (tripCount, route) -> {
                        double occupancy = (double) tripCount / route.getCapacity();
                        return "RouteId: " + route.getRouteId() + " Type: " + route.getType() + " Occupancy: " + occupancy;
                },
                Joined.with(Serdes.Long(), Serdes.Long(), new RouteSerde())
                );

                KTable<String, String> occupancyByTypeTable = occupancyStream
                .groupBy((key, value) -> { // Group by transport type
                        String[] parts = value.split(" Type: ");
                        if (parts.length > 1 && parts[1].contains(" ")) {
                        return parts[1].split(" ")[0];
                        }
                        System.err.println("Invalid data for grouping: " + value);
                        return "UNKNOWN";
                }, Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> "RouteId: NONE Occupancy: 9999.0",
                        (key, value, aggregate) -> {
                        try {
                                double currentOccupancy = Double.parseDouble(value.split(" Occupancy: ")[1]);
                                double aggregateOccupancy = Double.parseDouble(aggregate.split(" Occupancy: ")[1]);
                                return currentOccupancy < aggregateOccupancy ? value : aggregate;
                        } catch (Exception e) {
                                System.err.println("Error during aggregation: " + value + ", aggregate: " + aggregate);
                                return aggregate;
                        }
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .mapValues(value -> {
                        try {
                            Pattern pattern = Pattern.compile("RouteId:\\s*(\\d+)\\s*Type:\\s*\\w+\\s*Occupancy:\\s*([\\d\\.]+)");
                            Matcher matcher = pattern.matcher(value);
                    
                            if (matcher.find()) {
                                String routeId = matcher.group(1);
                                double occupancy = Double.parseDouble(matcher.group(2));
                    
                                return "RouteId: " + routeId + " -> Occupancy: " + occupancy;
                            } else {
                                throw new IllegalArgumentException("Formato inválido: " + value);
                            }
                        } catch (Exception e) {
                            System.err.println("Error formatting final value: " + value);
                            e.printStackTrace();
                            return "ERROR";
                        }
                    });                                    
                occupancyByTypeTable
                        .toStream()
                        .peek((key, value) -> System.out.println("Route with the least occupancy for transport type " + key + ": " + value))
                        .mapValues((key, value) -> createResult(key, value, "ResultsLeastOccupancyPerTransportType"))
                        .to("ResultsLeastOccupancyPerTransportType", Produced.with(Serdes.String(), new ResultsSerde()));
        
                // 13 => Get the most used transport type in the last hour using a tumbling window, return only one if there is a tie
                tripsStream
                        .groupBy((key, value) -> value.getTransportType(), Grouped.with(Serdes.String(), new TripSerde()))
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream()
                        .map((key, value) -> KeyValue.pair("mostused", key.key() + "->" + value))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .reduce(
                                (value1, value2) -> {
                                long count1 = Long.parseLong(value1.split("->")[1]);
                                long count2 = Long.parseLong(value2.split("->")[1]);
                                return count1 >= count2 ? value1 : value2;
                                },
                                Materialized.with(Serdes.String(), Serdes.String())
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Most used transport type in the last hour: " + value))
                        .mapValues((key, value) -> createResult(key, value, "ResultsMostUsedTransportType"))
                        .to("ResultsMostUsedTransportType", Produced.with(Serdes.String(), new ResultsSerde()));


                // 14 => Get the least occupied transport type in the last hour (use a tumbling time window)
                occupancyStream
                        .groupBy(
                                (key, value) -> value.split(" Type: ")[1].split(" ")[0],
                                Grouped.with(Serdes.String(), Serdes.String())
                        )
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
                        .aggregate(
                                () -> 0.0,
                                (key, value, aggregate) -> {
                                double currentOccupancy = Double.parseDouble(value.split(" Occupancy: ")[1]);
                                return aggregate + currentOccupancy;
                                },
                                Materialized.with(Serdes.String(), Serdes.Double())
                        )
                        .toStream()
                        .map((windowedKey, totalOccupancy) -> {
                                String transportType = windowedKey.key();
                                return KeyValue.pair("leastOccupied", transportType + " -> " + totalOccupancy);
                        })
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .reduce(
                                (aggValue, newValue) -> {
                                double aggOccupancy = Double.parseDouble(aggValue.split(" -> ")[1]);
                                double newOccupancy = Double.parseDouble(newValue.split(" -> ")[1]);
                                return newOccupancy < aggOccupancy ? newValue : aggValue;
                                },
                                Materialized.with(Serdes.String(), Serdes.String())
                        )
                        .toStream()
                        .map((key, value) -> {
                                String transportType = value.split(" -> ")[0];
                                String occupancy = value.split(" -> ")[1];
                                return KeyValue.pair("least occupied transport type", transportType + " -> " + occupancy);
                        })
                        .peek((key, value) -> System.out.println(key + ": " + value))
                        .mapValues((key, value) -> createResult(key, value, "ResultsLeastOccupiedTransportType"))
                        .to("ResultsLeastOccupiedTransportType", Produced.with(Serdes.String(), new ResultsSerde()));


                // 15 => Get the name of the route operator with the most occupancy. Include the value of occupancy
                KStream<Long, String> operatorOccupancyStream = tripCounts.toStream().join(
                routeTable,
                (tripCount, route) -> {
                        double occupancy = (double) tripCount / route.getCapacity();
                        return "Operator: " + route.getOperator() + " RouteId: " + route.getRouteId() + " Occupancy: " + occupancy;
                },
                Joined.with(Serdes.Long(), Serdes.Long(), new RouteSerde())
                );

                KTable<String, String> operatorMaxOccupancy = operatorOccupancyStream
                .groupBy(
                (key, value) -> value.split("Operator: ")[1].split(" ")[0],
                Grouped.with(Serdes.String(), Serdes.String())
                )
                .reduce(
                (aggValue, newValue) -> {
                        double aggOccupancy = Double.parseDouble(aggValue.split("Occupancy: ")[1]);
                        double newOccupancy = Double.parseDouble(newValue.split("Occupancy: ")[1]);
                        return newOccupancy > aggOccupancy ? newValue : aggValue;
                },
                Materialized.with(Serdes.String(), Serdes.String())
                );

                KStream<String, String> mostOccupiedOperatorStream = operatorMaxOccupancy
                .toStream()
                .groupBy(
                (key, value) -> "GlobalMaxOccupancy",
                Grouped.with(Serdes.String(), Serdes.String())
                )
                .reduce(
                (aggValue, newValue) -> {
                        double aggOccupancy = Double.parseDouble(aggValue.split("Occupancy: ")[1]);
                        double newOccupancy = Double.parseDouble(newValue.split("Occupancy: ")[1]);
                        return newOccupancy > aggOccupancy ? newValue : aggValue;
                },
                Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .map((key, value) -> {
                String operator = value.split("Operator: ")[1].split(" ")[0];
                double occupancy = Double.parseDouble(value.split("Occupancy: ")[1]);
                return KeyValue.pair("MostOccupiedOperator", operator + " -> " + occupancy);
                });

                mostOccupiedOperatorStream
                .peek((key, value) -> System.out.println("Most occupied operator: " + value))
                .mapValues((key, value) -> createResult(key, value, "MostOccupiedOperator"))
                .to("ResultsMostOccupiedOperator", Produced.with(Serdes.String(), new ResultsSerde()));


                // 16 => Get the name of the passenger with the most trips 
                tripsStream
                .groupBy((key, value) -> value.getPassengerName(), Grouped.with(Serdes.String(), new TripSerde()))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((key, value) -> KeyValue.pair("mosttrips", key + "->" + value))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce(
                        (value1, value2) -> {
                        long count1 = Long.parseLong(value1.split("->")[1]);
                        long count2 = Long.parseLong(value2.split("->")[1]);
                        return count1 >= count2 ? value1 : value2;
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .peek((key, value) -> System.out.println("Passenger with the most trips: " + value))
                .mapValues((key, value) -> createResult(key, value, "ResultsMostTripsPassenger"))
                .to("ResultsMostTripsPassenger", Produced.with(Serdes.String(), new ResultsSerde()));
        

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