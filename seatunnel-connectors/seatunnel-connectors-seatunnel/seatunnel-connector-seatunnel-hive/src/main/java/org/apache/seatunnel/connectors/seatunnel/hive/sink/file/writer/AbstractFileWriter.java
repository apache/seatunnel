package org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractFileWriter implements FileWriter {
    protected Map<String, String> needMoveFiles;
    protected SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;
    protected long sinkId;
    protected int subTaskIndex;
    protected HiveSinkConfig hiveSinkConfig;

    private static final String SEATUNNEL = "seatunnel";
    private static final String NON_PARTITION = "NON_PARTITION";

    protected Map<String, String> beingWrittenFile;

    protected String checkpointId;

    public AbstractFileWriter(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo,
                              HiveSinkConfig hiveSinkConfig,
                              long sinkId,
                              int subTaskIndex) {
        this.needMoveFiles = new HashMap<>();
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.sinkId = sinkId;
        this.subTaskIndex = subTaskIndex;
        this.hiveSinkConfig = hiveSinkConfig;

        this.beingWrittenFile = new HashMap<>();
    }

    public String getOrCreateFilePathBeingWritten(SeaTunnelRow seaTunnelRow) {
        String beingWrittenFileKey = getBeingWrittenFileKey(seaTunnelRow);
        // get filePath from beingWrittenFile
        String beingWrittenFilePath = beingWrittenFile.get(beingWrittenFileKey);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            StringBuilder sbf = new StringBuilder(hiveSinkConfig.getSinkTmpFsRootPath());
            sbf.append("/")
                .append(SEATUNNEL)
                .append("/")
                .append(sinkId)
                .append("/")
                .append(hiveSinkConfig.getHiveTableName())
                .append("/")
                .append(beingWrittenFileKey)
                .append("/")
                .append(sinkId)
                .append("_")
                .append(subTaskIndex)
                .append(".")
                .append(getFileSuffix());
            String newBeingWrittenFilePath = sbf.toString();
            beingWrittenFile.put(beingWrittenFileKey, newBeingWrittenFilePath);
            return newBeingWrittenFilePath;
        }
    }

    private String getBeingWrittenFileKey(SeaTunnelRow seaTunnelRow) {
        if (this.hiveSinkConfig.getPartitionFieldNames() != null && this.hiveSinkConfig.getPartitionFieldNames().size() > 0) {
            List<String> collect = this.hiveSinkConfig.getPartitionFieldNames().stream().map(partitionKey -> {
                StringBuilder sbd = new StringBuilder(partitionKey);
                sbd.append("=").append(seaTunnelRow.getFieldMap().get(partitionKey));
                return sbd.toString();
            }).collect(Collectors.toList());

            String beingWrittenFileKey = String.join("/", collect);
            return beingWrittenFileKey;
        } else {
            // If there is no partition field in data, We use the fixed value NON_PARTITION as the partition directory
            return NON_PARTITION;
        }
    }

    /**
     * FileWriter need return the file suffix. eg: tex, orc, parquet
     *
     * @return
     */
    public abstract String getFileSuffix();

    public String getHiveLocation(String seaTunnelFilePath) {
        StringBuilder sbf = new StringBuilder(hiveSinkConfig.getSinkTmpFsRootPath());
        sbf.append("/")
            .append(SEATUNNEL)
            .append("/")
            .append(sinkId)
            .append("/")
            .append(hiveSinkConfig.getHiveTableName());
        String seaTunnelPath = sbf.toString();
        String tmpPath = seaTunnelFilePath.replaceAll(seaTunnelPath, hiveSinkConfig.getHiveTableFsPath());
        return tmpPath.replaceAll(NON_PARTITION + "/", "");
    }
}
