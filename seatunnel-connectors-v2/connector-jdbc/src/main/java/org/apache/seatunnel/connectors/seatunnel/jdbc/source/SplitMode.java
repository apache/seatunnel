package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

public enum SplitMode {
    LEGACY("legacy"),

    CHARSET_BASED("charsetBased");

    public boolean equals(String mode) {
        return this.mode.equalsIgnoreCase(mode);
    }

    private final String mode;

    SplitMode(String mode) {
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
