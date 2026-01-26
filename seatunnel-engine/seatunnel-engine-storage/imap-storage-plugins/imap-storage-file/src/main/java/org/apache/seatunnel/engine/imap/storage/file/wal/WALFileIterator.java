package org.apache.seatunnel.engine.imap.storage.file.wal;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils.WAL_DATA_METADATA_LENGTH;

public class WALFileIterator implements IMapFileIterator {

    private final FSDataInputStream in;
    private final Serializer serializer;

    public WALFileIterator(FSDataInputStream in, Serializer serializer) {
        this.in = in;
        this.serializer = serializer;
    }

    @Override
    public boolean hasNext() throws IOException {
        return in.available() > WAL_DATA_METADATA_LENGTH;
    }

    @Override
    public IMapFileData next() throws IOException {
        byte[] meta = new byte[WAL_DATA_METADATA_LENGTH];
        in.readFully(meta);
        int len = WALDataUtils.byteArrayToInt(meta);

        byte[] data = new byte[len];
        in.readFully(data);

        return serializer.deserialize(data, IMapFileData.class);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
