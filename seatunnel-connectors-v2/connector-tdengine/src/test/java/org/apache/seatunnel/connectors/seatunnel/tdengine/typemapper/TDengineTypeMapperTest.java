package org.apache.seatunnel.connectors.seatunnel.tdengine.typemapper;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TDengineTypeMapperTest {

    @Test
    void mapping() {
        SeaTunnelDataType<?> type = TDengineTypeMapper.mapping("BOOL");
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, type);

        type = TDengineTypeMapper.mapping("CHAR");
        Assertions.assertEquals(BasicType.STRING_TYPE, type);
    }
}
