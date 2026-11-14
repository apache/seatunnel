package org.apache.seatunnel.azuredataexplorer.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class AzureDataExplorerSinkOptions {

    public static final Option<String> CLUSTER_URI =
            Options.key("cluster_uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "ADX cluster URI, e.g. https://mycluster.eastus.kusto.windows.net");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target database name.");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target table name.");

    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure AD application (client) ID.");

    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure AD application secret.");

    public static final Option<String> TENANT_ID =
            Options.key("tenant_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure AD tenant (directory) ID.");

    public static final Option<String> INGESTION_MAPPING_REFERENCE =
            Options.key("ingestion_mapping_reference")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Optional pre-created ingestion mapping name on the ADX table.");

    public static final Option<IngestionType> INGESTION_TYPE =
            Options.key("ingestion_type")
                    .enumType(IngestionType.class)
                    .defaultValue(IngestionType.QUEUED)
                    .withDescription(
                            "QUEUED (default, high throughput, ~5 min latency) or "
                                    + "STREAMING (low latency, <=4 MB/s per table).");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Rows to buffer before flushing.");

    public static final Option<Long> FLUSH_INTERVAL_MS =
            Options.key("flush_interval_ms")
                    .longType()
                    .defaultValue(30_000L)
                    .withDescription("Max milliseconds between flushes regardless of batch size.");

    public enum IngestionType {
        QUEUED,
        STREAMING
    }
}
