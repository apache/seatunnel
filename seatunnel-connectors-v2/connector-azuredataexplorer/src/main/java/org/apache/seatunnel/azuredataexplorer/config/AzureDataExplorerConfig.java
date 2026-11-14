package org.apache.seatunnel.azuredataexplorer.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Getter;

import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.BATCH_SIZE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.CLIENT_ID;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.CLIENT_SECRET;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.FLUSH_INTERVAL_MS;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.INGESTION_MAPPING_REFERENCE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.INGESTION_TYPE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.TABLE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.TENANT_ID;

/** Immutable config value object used by the sink. */
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
