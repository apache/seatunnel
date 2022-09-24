package org.apache.seatunnel.metrics.core;

public class SimpleGauge implements Gauge<Number>{
    private Number value;

    public SimpleGauge(Number value){
        this.value = value;
    }

    @Override
    public Number getValue() {
        return this.value;
    }

}
