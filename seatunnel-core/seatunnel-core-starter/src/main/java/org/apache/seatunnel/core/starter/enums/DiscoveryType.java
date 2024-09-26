package org.apache.seatunnel.core.starter.enums;

public enum DiscoveryType {
    LOCAL("local"),

    REMOTE("remote");

    private final String type;

    DiscoveryType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
