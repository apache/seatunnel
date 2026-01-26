package org.apache.seatunnel.engine.imap.storage.file.disruptor;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.dsl.Disruptor;

import java.io.Closeable;

public abstract class AbstractWALDisruptor implements Closeable {
    protected Disruptor<FileWALEvent> disruptor;

    protected static final int DEFAULT_RING_BUFFER_SIZE = 1024;

    protected static final int DEFAULT_CLOSE_WAIT_TIME_SECONDS = 5;

    protected volatile boolean isClosed = false;

    protected static final EventTranslatorThreeArg<FileWALEvent, IMapFileData, WALEventType, Long>
            TRANSLATOR =
                    (event, sequence, data, walEventStatus, requestId) -> {
                        event.setData(data);
                        event.setType(walEventStatus);
                        event.setRequestId(requestId);
                    };

    public boolean isClosed() {
        return isClosed;
    }

    public abstract boolean tryPublish(IMapFileData data, WALEventType type, long requestId);

    public abstract boolean tryAppendPublish(IMapFileData data, long requestId);
}
