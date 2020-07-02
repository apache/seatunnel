package io.github.interestinglab.waterdrop.utils;

public enum Engine {
    SPARK("spark"),FLINK("flink"),NULL("");

    private String engine;
    Engine(String engine) {
        this.engine = engine;
    }

    public String getEngine() {
        return engine;
    }
}
