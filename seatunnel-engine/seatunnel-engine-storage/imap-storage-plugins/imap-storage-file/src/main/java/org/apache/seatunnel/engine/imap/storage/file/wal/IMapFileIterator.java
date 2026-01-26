package org.apache.seatunnel.engine.imap.storage.file.wal;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import java.io.IOException;

public interface IMapFileIterator extends AutoCloseable {
    boolean hasNext() throws IOException;

    IMapFileData next() throws IOException;
}
