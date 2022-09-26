package org.apache.seatunnel.metrics.core;

public class SimpleMeter implements Meter{

    private double rate;
    private long count;

    public SimpleMeter(double rate, long count){
        this.rate = rate;
        this.count = count;
    }

    @Override
    public double getRate() {
        return this.rate;
    }

    @Override
    public long getCount() {
        return this.count;
    }
}
