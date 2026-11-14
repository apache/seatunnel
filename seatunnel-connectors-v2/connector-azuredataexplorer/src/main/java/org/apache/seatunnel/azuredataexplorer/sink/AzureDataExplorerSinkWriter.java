package org.apache.seatunnel.azuredataexplorer.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerConfig;
import org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.IngestionType;
import org.apache.seatunnel.azuredataexplorer.exception.AzureDataExplorerConnectorException;
import org.apache.seatunnel.azuredataexplorer.exception.AzureDataExplorerErrorCode;
import org.apache.seatunnel.azuredataexplorer.serialization.AzureDataExplorerRowSerializer;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class AzureDataExplorerSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private final AzureDataExplorerConfig config;
    private final AzureDataExplorerRowSerializer serializer;
    private final IngestClient ingestClient;
    private final IngestionProperties ingestionProperties;
    private final List<String> buffer;
    private long lastFlushTime;

    public AzureDataExplorerSinkWriter(AzureDataExplorerConfig config, SeaTunnelRowType rowType) {
        this(config, rowType, buildIngestClient(config));
    }

    AzureDataExplorerSinkWriter(
            AzureDataExplorerConfig config, SeaTunnelRowType rowType, IngestClient ingestClient) {
        this.config = config;
        this.serializer = new AzureDataExplorerRowSerializer(rowType);
        this.buffer = new ArrayList<>(Math.max(config.getBatchSize(), 16));
        this.lastFlushTime = System.currentTimeMillis();
        this.ingestClient = ingestClient;
        this.ingestionProperties = buildIngestionProperties(config);
    }

    private static IngestClient buildIngestClient(AzureDataExplorerConfig config) {
        try {
            String uri =
                    config.getIngestionType() == IngestionType.STREAMING
                            ? config.getClusterUri()
                            : config.getQueuedIngestUri();
            ConnectionStringBuilder csb =
                    ConnectionStringBuilder.createWithAadApplicationCredentials(
                            uri,
                            config.getClientId(),
                            config.getClientSecret(),
                            config.getTenantId());
            return config.getIngestionType() == IngestionType.STREAMING
                    ? IngestClientFactory.createStreamingIngestClient(csb)
                    : IngestClientFactory.createClient(csb);
        } catch (Exception e) {
            throw new AzureDataExplorerConnectorException(
                    AzureDataExplorerErrorCode.CONNECTION_FAILED,
                    "Cannot create ADX ingest client for cluster: " + config.getClusterUri(),
                    e);
        }
    }

    private static IngestionProperties buildIngestionProperties(AzureDataExplorerConfig config) {
        IngestionProperties props =
                new IngestionProperties(config.getDatabase(), config.getTable());
        props.setDataFormat(IngestionProperties.DataFormat.CSV);
        props.setIgnoreFirstRecord(false);
        String mappingRef = config.getIngestionMappingReference();
        if (mappingRef != null && !mappingRef.isEmpty()) {
            props.setIngestionMapping(
                    new IngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.CSV));
        }
        return props;
    }

    @Override
    public void write(SeaTunnelRow element) {
        buffer.add(serializer.toCsvLine(element));
        boolean batchFull = buffer.size() >= config.getBatchSize();
        boolean timedOut =
                System.currentTimeMillis() - lastFlushTime >= config.getFlushIntervalMs();
        if (batchFull || timedOut) flush();
    }

    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    @Override
    public List<Void> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void abortPrepare() {
        buffer.clear();
    }

    @Override
    public void close() {
        try {
            if (!buffer.isEmpty()) flush();
        } finally {
            try {
                ingestClient.close();
            } catch (Exception e) {
                log.warn("Error closing ADX ingest client", e);
            }
        }
    }

    private void flush() {
        if (buffer.isEmpty()) return;
        StringBuilder sb = new StringBuilder();
        for (String line : buffer) sb.append(line);
        byte[] csv = sb.toString().getBytes(StandardCharsets.UTF_8);
        StreamSourceInfo si = new StreamSourceInfo(new ByteArrayInputStream(csv));
        try {
            ingestClient.ingestFromStream(si, ingestionProperties);
            log.debug(
                    "Flushed {} rows to {}.{}",
                    buffer.size(),
                    config.getDatabase(),
                    config.getTable());
        } catch (Exception e) {
            throw new AzureDataExplorerConnectorException(
                    AzureDataExplorerErrorCode.INGESTION_FAILED,
                    "Ingestion failed for " + buffer.size() + " rows",
                    e);
        } finally {
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();
        }
    }
}
