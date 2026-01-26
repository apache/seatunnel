package org.apache.seatunnel.engine.imap.storage.file.disruptor;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALLSMWriter;
import org.apache.seatunnel.engine.imap.storage.file.common.WALWriter;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WALCompactionDisruptor extends AbstractWALDisruptor {

    private Disruptor<FileWALEvent> compactionDisruptor;

    public WALCompactionDisruptor(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            String parentPath,
            Serializer serializer,
            Map<String, Object> config) {
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        this.disruptor =
                new Disruptor<>(
                        FileWALEvent.FACTORY,
                        DEFAULT_RING_BUFFER_SIZE,
                        threadFactory,
                        ProducerType.SINGLE,
                        new BlockingWaitStrategy());

        WALWriter writer;
        try {
            writer =
                    new WALLSMWriter(
                            fs, fileConfiguration, new Path(parentPath), serializer, config);
        } catch (IOException e) {
            throw new IMapStorageException(
                    e, "create new current writer failed, parent path is %s", parentPath);
        }

        disruptor.handleEventsWithWorkerPool(new WALWorkHandler(writer));

        disruptor.start();

        this.compactionDisruptor =
                new Disruptor<>(
                        FileWALEvent.FACTORY,
                        DEFAULT_RING_BUFFER_SIZE,
                        threadFactory,
                        ProducerType.SINGLE,
                        new BlockingWaitStrategy());

        compactionDisruptor.handleEventsWithWorkerPool(
                new WALCompactionWorkHandler((WALLSMWriter) writer));

        compactionDisruptor.start();
    }

    @Override
    public boolean tryPublish(IMapFileData message, WALEventType status, long requestId) {
        if (isClosed()) {
            return false;
        }
        disruptor.getRingBuffer().publishEvent(TRANSLATOR, message, status, requestId);
        compactionDisruptor.getRingBuffer().publishEvent(TRANSLATOR, message, status, requestId);
        return true;
    }

    @Override
    public boolean tryAppendPublish(IMapFileData message, long requestId) {
        return this.tryPublish(message, WALEventType.APPEND, requestId);
    }

    @Override
    public void close() throws IOException {
        // we can wait for 5 seconds, so that backlog can be committed
        try {
            tryPublish(null, WALEventType.CLOSED, 0L);
            isClosed = true;
            disruptor.shutdown(DEFAULT_CLOSE_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
            compactionDisruptor.shutdown(DEFAULT_CLOSE_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("WALCompactionDisruptor close timeout error", e);
            throw new IMapStorageException("WALCompactionDisruptor close timeout error", e);
        }
    }
}
