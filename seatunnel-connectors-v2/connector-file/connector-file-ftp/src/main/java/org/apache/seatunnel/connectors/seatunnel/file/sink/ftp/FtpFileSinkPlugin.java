package org.apache.seatunnel.connectors.seatunnel.file.sink.ftp;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.ftp.filesystem.FtpFileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.sink.ftp.filesystem.FtpFileSystemCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.ftp.writer.FtpTransactionStateFileWriteFactory;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystemCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.SinkFileSystemPlugin;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.PartitionDirNameGenerator;

import lombok.NonNull;

import java.util.List;
import java.util.Optional;

public class FtpFileSinkPlugin implements SinkFileSystemPlugin {

    @Override
    public String getPluginName() {

        return FileSystemType.FTP.getSinkFileSystemPluginName();
    }

    @Override
    public Optional<TransactionStateFileWriter> getTransactionStateFileWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo, @NonNull TransactionFileNameGenerator transactionFileNameGenerator, @NonNull PartitionDirNameGenerator partitionDirNameGenerator, @NonNull List<Integer> sinkColumnsIndexInRow, @NonNull String tmpPath, @NonNull String targetPath, @NonNull String jobId, int subTaskIndex, @NonNull String fieldDelimiter, @NonNull String rowDelimiter, @NonNull FileSystem fileSystem) {

        TransactionStateFileWriter writer = FtpTransactionStateFileWriteFactory.of(seaTunnelRowTypeInfo, transactionFileNameGenerator, partitionDirNameGenerator, sinkColumnsIndexInRow, tmpPath, targetPath, jobId, subTaskIndex, fieldDelimiter, rowDelimiter, fileSystem);

        return Optional.of(writer);
    }

    @Override
    public Optional<FileSystemCommitter> getFileSystemCommitter() {
        return Optional.of(new FtpFileSystemCommitter());
    }

    @Override
    public Optional<FileSystem> getFileSystem() {
        return Optional.of(new FtpFileSystem());
    }
}
