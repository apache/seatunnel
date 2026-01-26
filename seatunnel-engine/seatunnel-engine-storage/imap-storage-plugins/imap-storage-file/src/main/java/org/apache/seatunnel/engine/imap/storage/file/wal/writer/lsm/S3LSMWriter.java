package org.apache.seatunnel.engine.imap.storage.file.wal.writer.lsm;

import java.util.Map;

public class S3LSMWriter extends CloudLSMWriter {
    public S3LSMWriter(Map<String, Object> config) {
        super(config);
    }

    @Override
    public String identifier() {
        return "s3";
    }
}
