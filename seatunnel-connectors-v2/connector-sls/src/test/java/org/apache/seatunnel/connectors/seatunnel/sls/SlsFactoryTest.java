package org.apache.seatunnel.connectors.seatunnel.sls;

import org.apache.seatunnel.connectors.seatunnel.sls.source.SlsSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SlsFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new SlsSourceFactory()).optionRule());
    }
}
