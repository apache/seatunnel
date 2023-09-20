package org.apache.seatunnel.transform.sql.zeta.agg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OutputFields implements OutputAggregate<Object> {
    private final List<Object> outputFields = new ArrayList<>();
    private int index = -1;

    @Override
    public Object getTail() {
        return outputFields.get(index);
    }

    @Override
    public void add(Object item) {
        outputFields.add(item);
        index++;
    }

    @Override
    public void addOrReplaceTail(int index, Object item) {
        if (outputFields.size() == index + 1 && index >= 0) {
            outputFields.set(index, item);
            this.index = index;
        } else {
            outputFields.add(item);
            this.index++;
        }
    }

    @Override
    public int getIndex() {
        return this.index;
    }

    @Override
    public void addAll(Object[] arr) {
        Collections.addAll(outputFields, arr);
        this.index = arr.length - 1;
    }

    @Override
    public Object[] toArray() {
        return outputFields.toArray(new Object[index + 1]);
    }

    @Override
    public void clean() {
        outputFields.clear();
        index = -1;
    }
}
