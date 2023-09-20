package org.apache.seatunnel.transform.sql.zeta.agg;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OutputDataTypes implements OutputAggregate<SeaTunnelDataType<?>> {
    private final List<SeaTunnelDataType<?>> outputDataTypes = new ArrayList<>();
    private int index = -1;

    @Override
    public SeaTunnelDataType<?> getTail() {
        return outputDataTypes.get(index);
    }

    @Override
    public void add(SeaTunnelDataType<?> item) {
        outputDataTypes.add(item);
        index++;
    }

    @Override
    public void addOrReplaceTail(int index, SeaTunnelDataType<?> item) {
        if (outputDataTypes.size() == index + 1 && index >= 0) {
            outputDataTypes.set(index, item);
            this.index = index;
        } else {
            outputDataTypes.add(item);
            this.index++;
        }
    }

    @Override
    public int getIndex() {
        return this.index;
    }

    @Override
    public void addAll(SeaTunnelDataType<?>[] arr) {
        Collections.addAll(outputDataTypes, arr);
        this.index = arr.length - 1;
    }

    @Override
    public SeaTunnelDataType<?>[] toArray() {
        return outputDataTypes.toArray(new SeaTunnelDataType<?>[index + 1]);
    }

    @Override
    public void clean() {
        outputDataTypes.clear();
        index = -1;
    }
}
