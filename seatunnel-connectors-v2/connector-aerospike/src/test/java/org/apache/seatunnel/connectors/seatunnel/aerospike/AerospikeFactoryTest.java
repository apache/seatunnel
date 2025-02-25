package org.apache.seatunnel.connectors.seatunnel.aerospike;

import org.apache.seatunnel.connectors.seatunnel.aerospike.sink.AerospikeSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AerospikeFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new AerospikeSinkFactory()).optionRule());
        Assertions.assertNotNull((new AerospikeSinkFactory()).optionRule());
    }
}
