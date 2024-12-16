package tp3.kafka.serdes.results;

import org.apache.kafka.common.serialization.Serdes;

import tp3.persistence.entity.Results;

public class ResultsSerde extends Serdes.WrapperSerde<Results> {

    public ResultsSerde() {
        super(new ResultsSerializer(), null);
    }
}
