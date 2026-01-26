package org.apache.seatunnel.engine.imap.storage.file.disruptor;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALLSMWriter;

import com.lmax.disruptor.WorkHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class WALCompactionWorkHandler implements WorkHandler<FileWALEvent> {

    private WALLSMWriter writer;

    public WALCompactionWorkHandler(WALLSMWriter writer) {
        this.writer = writer;
    }

    @Override
    public void onEvent(FileWALEvent fileWALEvent) {
        log.debug("write data to orc file");
        walEvent(fileWALEvent.getData(), fileWALEvent.getType());
    }

    private void walEvent(IMapFileData iMapFileData, WALEventType type) {
        if (type == WALEventType.APPEND) {
            try {
                writer.compaction();
            } catch (IOException e) {
                log.warn("compact orc file error, walEventBean is {} ", iMapFileData, e);
            }
        }
    }
}
