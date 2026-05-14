package org.apache.seatunnel.azuredataexplorer.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class AzureDataExplorerConfig {
    public static final Option<String> CLUSTER_URL =
            Options.key("cluster_url").stringType().noDefaultValue()
                    .withDescription("ADX cluster URI e.g. https://mycluster.eastus.kusto.windows.net");

    public static final Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue()
                    .withDescription("Database name");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue()
                    .withDescription("Table name");

    public static final Option<String> CLIENT_ID =
            Options.key("client_id").stringType().noDefaultValue()
                    .withDescription("Azure AD service principal client ID");

    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret").stringType().noDefaultValue()
                    .withDescription("Azure AD service principal client secret");

    public static final Option<String> TENANT_ID =
            Options.key("tenant_id").stringType().noDefaultValue()
                    .withDescription("Azure AD tenant ID");

    public static final Option<String> QUERY =
            Options.key("query").stringType().noDefaultValue()
                    .withDescription("KQL query for reading, e.g. MyTable | take 1000");

    public static final Option<Boolean> USE_STREAMING_INGEST =
            Options.key("use_streaming_ingest").booleanType().defaultValue(false)
                    .withDescription("Use streaming ingestion (must be enabled on cluster)");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size").intType().defaultValue(1000)
                    .withDescription("Row buffer size before flushing to ADX");
}