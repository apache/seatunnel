package org.apache.seatunnel.format.sensorsdata;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class SensorsDataTypesTest {

    @Test
    public void of() {
        SensorsDataTypes type = SensorsDataTypes.of("TIMESTAMP yyyy-MM-dd'T'HH:mm:ssZ");
        Assertions.assertEquals(SensorsDataTypes.DataTypes.TIMESTAMP, type.getType());
        Assertions.assertEquals("yyyy-MM-dd'T'HH:mm:ssZ", type.getExtra());
    }
}
