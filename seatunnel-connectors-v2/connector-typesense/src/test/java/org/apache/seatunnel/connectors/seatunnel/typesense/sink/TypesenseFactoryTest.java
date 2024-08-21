package org.apache.seatunnel.connectors.seatunnel.typesense.sink;


import org.apache.seatunnel.connectors.seatunnel.typesense.source.TypesenseSourceFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TypesenseFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new TypesenseSourceFactory()).optionRule());
        Assertions.assertNotNull((new TypesenseSinkFactory()).optionRule());
    }
}
