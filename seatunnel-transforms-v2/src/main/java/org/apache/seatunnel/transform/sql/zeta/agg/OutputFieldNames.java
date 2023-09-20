package org.apache.seatunnel.transform.sql.zeta.agg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OutputFieldNames implements OutputAggregate<String> {
    private final List<String> outputFieldNames = new ArrayList<>();
    private int index = -1;

    @Override
    public String getTail() {
        return outputFieldNames.get(index);
    }

    @Override
    public void add(String item) {
        outputFieldNames.add(item);
        index++;
    }

    @Override
    public void addOrReplaceTail(int index, String item) {
        if (outputFieldNames.size() == index + 1 && index >= 0) {
            outputFieldNames.set(index, item);
            this.index = index;
        } else {
            outputFieldNames.add(item);
            this.index++;
        }
    }

    @Override
    public int getIndex() {
        return this.index;
    }

    @Override
    public void addAll(String[] arr) {
        Collections.addAll(outputFieldNames, arr);
        this.index = arr.length - 1;
    }

    @Override
    public String[] toArray() {
        return outputFieldNames.toArray(new String[index + 1]);
    }

    @Override
    public void clean() {
        outputFieldNames.clear();
        index = -1;
    }
}
