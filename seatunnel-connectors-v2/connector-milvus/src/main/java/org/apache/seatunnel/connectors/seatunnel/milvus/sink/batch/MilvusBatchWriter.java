package org.apache.seatunnel.connectors.seatunnel.milvus.sink.batch;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public interface MilvusBatchWriter {

    void addToBatch(SeaTunnelRow element);

    boolean needFlush();

    void flush() throws Exception;

    void close() throws Exception;
}
