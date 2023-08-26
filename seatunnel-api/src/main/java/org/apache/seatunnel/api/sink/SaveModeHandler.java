package org.apache.seatunnel.api.sink;

public interface SaveModeHandler extends AutoCloseable {

    void handleSchemaSaveMode();

    void handleDataSaveMode();

    default void handleSaveMode() {
        handleSchemaSaveMode();
        handleDataSaveMode();
    }
}
