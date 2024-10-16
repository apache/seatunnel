package org.apache.seatunnel.connectors.tencent.vectordb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class TencentVectorDBSourceConfig {
    public static final String CONNECTOR_IDENTITY = "TencentVectorDB";

    public static final Option<String> URL =
            Options.key("url").stringType().noDefaultValue().withDescription("url");

    public static final Option<String> USER_NAME =
            Options.key("user_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("user name for authentication");

    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("token for authentication");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tencent Vector DB database name");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tencent Vector DB collection name");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Tencent Vector DB reader batch size");
}
