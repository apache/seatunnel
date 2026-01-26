package org.apache.seatunnel.engine.imap.storage.file.wal.writer.lsm;

import java.util.Map;

public class OssLSMWriter extends CloudLSMWriter {
    public OssLSMWriter(Map<String, Object> config) {
        super(config);
    }

    @Override
    public String identifier() {
        return "oss";
    }
}
