package org.apache.seatunnel.azuredataexplorer.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceConfig;
import org.apache.seatunnel.azuredataexplorer.exception.AzureDataExplorerConnectorException;
import org.apache.seatunnel.azuredataexplorer.exception.AzureDataExplorerErrorCode;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultColumn;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import lombok.extern.slf4j.Slf4j;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

@Slf4j
public class AzureDataExplorerSourceReader
        implements SourceReader<SeaTunnelRow, AzureDataExplorerSourceSplit> {

    private final SourceReader.Context context;
    private final AzureDataExplorerSourceConfig config;
    private final Queue<AzureDataExplorerSourceSplit> splitQueue;
    private SeaTunnelRowType rowType;
    private Client client;
    private volatile boolean noMoreSplits;

    AzureDataExplorerSourceReader(
            SourceReader.Context context, ReadonlyConfig config, SeaTunnelRowType rowType) {
        this.context = context;
        this.config = AzureDataExplorerSourceConfig.fromSourceConfig(config);
        this.rowType = rowType;
        this.splitQueue = new ArrayDeque<>();
    }

    @Override
    public void open() {
        try {
            ConnectionStringBuilder csb =
                    ConnectionStringBuilder.createWithAadApplicationCredentials(
                            config.getClusterUri(),
                            config.getClientId(),
                            config.getClientSecret(),
                            config.getTenantId());
            this.client = ClientFactory.createClient(csb);
        } catch (Exception e) {
            throw new AzureDataExplorerConnectorException(
                    AzureDataExplorerErrorCode.CONNECTION_FAILED,
                    "Cannot create ADX data client for cluster: " + config.getClusterUri(),
                    e);
        }
    }

    @Override
    public void close() {
        // No close required for Kusto data client.
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        synchronized (output.getCheckpointLock()) {
            AzureDataExplorerSourceSplit split = splitQueue.poll();
            if (split != null) {
                executeQuery(output);
            } else if (noMoreSplits) {
                log.info("Closed the bounded Azure Data Explorer source");
                context.signalNoMoreElement();
            }
        }
    }

    private void executeQuery(Collector<SeaTunnelRow> output) {
        try {
            KustoOperationResult result =
                    client.executeQuery(config.getDatabase(), config.getQuery());
            KustoResultSetTable table = result.getPrimaryResults();
            if (rowType == null) {
                rowType = buildRowType(table.getColumns());
            }
            while (table.next()) {
                List<Object> currentRow = table.getCurrentRow();
                output.collect(new SeaTunnelRow(convertRow(currentRow, rowType)));
            }
        } catch (Exception e) {
            throw new AzureDataExplorerConnectorException(
                    AzureDataExplorerErrorCode.QUERY_FAILED,
                    "Failed to execute query on database " + config.getDatabase(),
                    e);
        }
    }

    private Object[] convertRow(List<Object> row, SeaTunnelRowType rowType) {
        Object[] converted = new Object[row.size()];
        SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();
        for (int i = 0; i < row.size(); i++) {
            Object value = row.get(i);
            SeaTunnelDataType<?> type = fieldTypes[i];
            converted[i] = convertValue(value, type);
        }
        return converted;
    }

    private Object convertValue(Object value, SeaTunnelDataType<?> type) {
        if (value == null) {
            return null;
        }
        if (type == LocalTimeType.LOCAL_DATE_TYPE) {
            if (value instanceof Date) {
                return ((Date) value).toLocalDate();
            }
            if (value instanceof OffsetDateTime) {
                return ((OffsetDateTime) value).toLocalDate();
            }
        }
        if (type == LocalTimeType.LOCAL_DATE_TIME_TYPE) {
            if (value instanceof Timestamp) {
                return ((Timestamp) value).toLocalDateTime();
            }
            if (value instanceof OffsetDateTime) {
                return ((OffsetDateTime) value).toLocalDateTime();
            }
            if (value instanceof Instant) {
                return LocalDateTime.ofInstant((Instant) value, ZoneOffset.UTC);
            }
        }
        return value;
    }

    private SeaTunnelRowType buildRowType(KustoResultColumn[] columns) {
        String[] names = new String[columns.length];
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType<?>[columns.length];
        for (int i = 0; i < columns.length; i++) {
            names[i] = columns[i].getColumnName();
            types[i] = mapKustoType(columns[i].getColumnType());
        }
        return new SeaTunnelRowType(names, types);
    }

    private SeaTunnelDataType<?> mapKustoType(String kustoType) {
        if (kustoType == null) {
            return BasicType.STRING_TYPE;
        }
        String normalizedType = kustoType.toLowerCase();
        if ("string".equals(normalizedType)) {
            return BasicType.STRING_TYPE;
        }
        if ("int".equals(normalizedType)) {
            return BasicType.INT_TYPE;
        }
        if ("long".equals(normalizedType)) {
            return BasicType.LONG_TYPE;
        }
        if ("real".equals(normalizedType)) {
            return BasicType.DOUBLE_TYPE;
        }
        if ("bool".equals(normalizedType)) {
            return BasicType.BOOLEAN_TYPE;
        }
        if ("datetime".equals(normalizedType)) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        }
        if ("timespan".equals(normalizedType)
                || "guid".equals(normalizedType)
                || "dynamic".equals(normalizedType)) {
            return BasicType.STRING_TYPE;
        }
        if ("decimal".equals(normalizedType)) {
            return new DecimalType(38, 18);
        }
        return BasicType.STRING_TYPE;
    }

    @Override
    public List<AzureDataExplorerSourceSplit> snapshotState(long checkpointId) {
        return List.copyOf(splitQueue);
    }

    @Override
    public void addSplits(List<AzureDataExplorerSourceSplit> splits) {
        splitQueue.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
