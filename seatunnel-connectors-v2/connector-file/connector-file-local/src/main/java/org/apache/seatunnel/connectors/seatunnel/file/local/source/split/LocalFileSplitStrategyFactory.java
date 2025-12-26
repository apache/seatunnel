package org.apache.seatunnel.connectors.seatunnel.file.local.source.split;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.DefaultFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.ParquetFileSplitStrategy;

import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions.DEFAULT_ROW_DELIMITER;

public class LocalFileSplitStrategyFactory {

    public static FileSplitStrategy initFileSplitStrategy(ReadonlyConfig readonlyConfig) {
        if (!readonlyConfig.get(FileBaseSourceOptions.ENABLE_FILE_SPLIT)) {
            return new DefaultFileSplitStrategy();
        }
        if (!readonlyConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE).supportFileSplit()) {
            return new DefaultFileSplitStrategy();
        }
        if (readonlyConfig.get(FileBaseSourceOptions.COMPRESS_CODEC) != CompressFormat.NONE
                || readonlyConfig.get(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC)
                        != ArchiveCompressFormat.NONE) {
            return new DefaultFileSplitStrategy();
        }
        long fileSplitSize = readonlyConfig.get(FileBaseSourceOptions.FILE_SPLIT_SIZE);
        if (FileFormat.PARQUET == readonlyConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE)) {
            return new ParquetFileSplitStrategy(fileSplitSize);
        }
        String rowDelimiter =
                !readonlyConfig.getOptional(FileBaseSourceOptions.ROW_DELIMITER).isPresent()
                        ? DEFAULT_ROW_DELIMITER
                        : readonlyConfig.get(FileBaseSourceOptions.ROW_DELIMITER);
        long skipHeaderRowNumber =
                readonlyConfig.get(FileBaseSourceOptions.CSV_USE_HEADER_LINE)
                        ? 1L
                        : readonlyConfig.get(FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER);
        String encodingName = readonlyConfig.get(FileBaseSourceOptions.ENCODING);
        return new LocalFileAccordingToSplitSizeSplitStrategy(
                rowDelimiter, skipHeaderRowNumber, encodingName, fileSplitSize);
    }
}
