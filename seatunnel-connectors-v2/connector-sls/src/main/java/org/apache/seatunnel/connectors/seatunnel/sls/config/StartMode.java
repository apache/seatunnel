package org.apache.seatunnel.connectors.seatunnel.sls.config;

public enum StartMode {
    EARLIEST("earliest"),

    GROUP_OFFSETS("group_offsets"),

    LATEST("latest");

    private String mode;

    StartMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return mode;
    }
}
