package org.apache.seatunnel.connectors.seatunnel.neo4j.internal;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.AsValue;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.MapValue;

import java.util.Map;

/**
 * This class includes the seatunnelRow and implements the neo4j.driver.internal.AsValue interface.
 * This class will be able to convert to neo4j.driver.Value quickly without any extra effort.
 */
public class SeatunnelRowNeo4jValue implements AsValue {
    private final SeaTunnelRowType seaTunnelRowType;
    private final SeaTunnelRow seaTunnelRow;

    public SeatunnelRowNeo4jValue(SeaTunnelRowType seaTunnelRowType, SeaTunnelRow seaTunnelRow) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.seaTunnelRow = seaTunnelRow;
    }

    @Override
    public Value asValue() {
        int length = seaTunnelRowType.getTotalFields();
        Map<String, Value> valueMap = Iterables.newHashMapWithSize(length);
        for (int i = 0; i < length; i++) {
            String name = seaTunnelRowType.getFieldName(i);
            Value value = Values.value(seaTunnelRow.getField(i));
            valueMap.put(name, value);
        }
        return new MapValue(valueMap);
    }
}
