package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.scan;

import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset;

import io.debezium.pipeline.source.spi.ChangeEventSource;

/**
 * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high watermark
 * for each {@link SnapshotSplit}.
 */
public class SnapshotSplitChangeEventSourceContext
        implements ChangeEventSource.ChangeEventSourceContext {

    private RedoLogOffset lowWatermark;
    private RedoLogOffset highWatermark;

    public RedoLogOffset getLowWatermark() {
        return lowWatermark;
    }

    public void setLowWatermark(RedoLogOffset lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    public RedoLogOffset getHighWatermark() {
        return highWatermark;
    }

    public void setHighWatermark(RedoLogOffset highWatermark) {
        this.highWatermark = highWatermark;
    }

    @Override
    public boolean isRunning() {
        return lowWatermark != null && highWatermark != null;
    }
}
