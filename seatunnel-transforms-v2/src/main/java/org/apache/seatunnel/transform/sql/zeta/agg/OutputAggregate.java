package org.apache.seatunnel.transform.sql.zeta.agg;

public interface OutputAggregate<T> {
    T getTail();

    void add(T item);

    void addOrReplaceTail(int index, T item);

    int getIndex();

    void addAll(T[] arr);

    T[] toArray();

    void clean();
}
