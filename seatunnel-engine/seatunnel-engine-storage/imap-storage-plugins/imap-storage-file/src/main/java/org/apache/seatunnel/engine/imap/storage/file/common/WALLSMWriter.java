package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.wal.DiscoveryWalFileFactory;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

public class WALLSMWriter extends WALWriter {
    public WALLSMWriter(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            Path parentPath,
            Serializer serializer,
            Map<String, Object> config)
            throws IOException {
        super(fs, fileConfiguration, parentPath, serializer);
        this.writer = DiscoveryWalFileFactory.getLSMWriter(fileConfiguration.getName(), config);
        this.writer.setBlockSize(fileConfiguration.getConfiguration().getBlockSize());
        this.writer.initialize(fs, parentPath, serializer);
    }

    public void compaction() throws IOException {
        this.writer.compaction(false);
    }
}
