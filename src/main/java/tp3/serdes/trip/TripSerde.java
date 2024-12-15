package tp3.serdes.trip;

import org.apache.kafka.common.serialization.Serdes;

import tp3.persistence.entity.Trip;

public class TripSerde extends Serdes.WrapperSerde<Trip> {
    public TripSerde() {
        super(new TripSerializer(), new TripDeserializer());
    }
}
