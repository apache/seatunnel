package org.apache.seatunnel.connectors.seatunnel.sls.config;

public enum StartMode {
    EARLIEST("earliest"),

    GROUP_CURSOR("group_cursor"),

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
