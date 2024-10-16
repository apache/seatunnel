package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MilvusSinkFactoryTest {
    private final MilvusSinkFactory milvusSinkFactory = new MilvusSinkFactory();

    @Test
    void factoryIdentifier() {
        Assertions.assertEquals(
                milvusSinkFactory.factoryIdentifier(),
                MilvusSinkConfig.CONNECTOR_IDENTITY.toString());
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull(milvusSinkFactory.optionRule());
    }
}
