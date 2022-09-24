package org.apache.seatunnel.metrics.core;

public class SimpleCounter implements Counter {
    private long count;

    public SimpleCounter(long count) {
        this.count = count;
    }

    public void inc() {
        ++this.count;
    }

    public void inc(long n) {
        this.count += n;
    }

    public void dec() {
        --this.count;
    }

    public void dec(long n) {
        this.count -= n;
    }

    public long getCount() {
        return this.count;
    }
}
