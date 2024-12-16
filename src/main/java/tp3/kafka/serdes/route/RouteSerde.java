package tp3.kafka.serdes.route;

import org.apache.kafka.common.serialization.Serdes;

import tp3.persistence.entity.Route;

public class RouteSerde extends Serdes.WrapperSerde<Route> {
    public RouteSerde() {
        super(new RouteSerializer(), new RouteDeserializer());
    }
}