package serdes.trip;

import entity.Trip;
import org.apache.kafka.common.serialization.Serdes;

public class TripSerde extends Serdes.WrapperSerde<Trip> {
    public TripSerde() {
        super(new TripSerializer(), new TripDeserializer());
    }
}
