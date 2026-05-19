package org.apache.seatunnel.azuredataexplorer.config;

import lombok.Builder;
import lombok.Getter;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import static org.apache.seatunnel.api.options.table.TableIdentifierOptions.TABLE;
import static org.apache.seatunnel.azuredataexplorer.config
        .AzureDataExplorerSinkOptions.*;

/** Immutable config value object used by both sink and source. */
@Getter
@Builder
public class AzureDataExplorerConfig {

    private final String clusterUri;
    private final String database;
    private final String clientId;
    private final String clientSecret;
    private final String tenantId;

    private final String table;
    private final String ingestionMappingReference;
    private final AzureDataExplorerSinkOptions.IngestionType ingestionType;
    private final int batchSize;
    private final long flushIntervalMs;

    public String getQueuedIngestUri() {
        if (clusterUri.startsWith("https://")) {
            return "https://ingest-" + clusterUri.substring("https://".length());
        }
        return clusterUri;
    }

    public static AzureDataExplorerConfig fromSinkConfig(ReadonlyConfig cfg) {
        return AzureDataExplorerConfig.builder()
                .clusterUri(cfg.get(AzureDataExplorerSinkOptions.CLUSTER_URI))
                .database(cfg.get(AzureDataExplorerSinkOptions.DATABASE))
                .table(cfg.get(TABLE))
                .clientId(cfg.get(CLIENT_ID))
                .clientSecret(cfg.get(CLIENT_SECRET))
                .tenantId(cfg.get(TENANT_ID))
                .ingestionMappingReference(cfg.get(INGESTION_MAPPING_REFERENCE))
                .ingestionType(cfg.get(INGESTION_TYPE))
                .batchSize(cfg.get(BATCH_SIZE))
                .flushIntervalMs(cfg.get(FLUSH_INTERVAL_MS))
                .build();
    }
}