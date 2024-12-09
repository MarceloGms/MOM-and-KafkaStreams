package serdes.route;

import entity.Route;
import org.apache.kafka.common.serialization.Serdes;

public class RouteSerde extends Serdes.WrapperSerde<Route> {
    public RouteSerde() {
        super(new RouteSerializer(), new RouteDeserializer());
    }
}