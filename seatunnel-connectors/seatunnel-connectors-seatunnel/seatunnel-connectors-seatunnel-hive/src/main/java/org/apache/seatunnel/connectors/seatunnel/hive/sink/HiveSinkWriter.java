package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class HiveSinkWriter implements SinkWriter<SeaTunnelRow, Object, Object> {
    @Override
    public void write(SeaTunnelRow element) throws IOException {
        //TODO
    }

    @Override
    public Optional prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public List snapshotState() throws IOException {
        return SinkWriter.super.snapshotState();
    }

    @Override
    public void abort() {

    }

    @Override
    public void close() throws IOException {

    }
}
