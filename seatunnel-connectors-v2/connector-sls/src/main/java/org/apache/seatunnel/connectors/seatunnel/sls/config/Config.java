package org.apache.seatunnel.connectors.seatunnel.sls.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class Config {
    public static final String CONNECTOR_IDENTITY = "Sls";

    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Endpoint to connect to");
    public static final Option<String> PROJECT =
            Options.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sls project to connect to");

    public static final Option<String> LOGSTORE =
            Options.key("logstore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sls logstore to connect to");

    public static final Option<String> CONSUMER_GROUP =
            Options.key("consumer_group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sls consumer group to connect to");

    public static final Option<String> ACCESS_KEY_ID =
            Options.key("access_key_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sls access_key_id");

    public static final Option<String> ACCESS_KEY_SECRET =
            Options.key("access_key_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sls access_key_secret");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size").intType().defaultValue(1000).withDescription("sls batch_size");
    public static final Option<Integer> CONSUMER_NUM =
            Options.key("consumer_num")
                    .intType()
                    .defaultValue(3)
                    .withDescription("sls consumer number");

    public static final Option<StartMode> POSITION_MODE =
            Options.key("position_mode")
                    .objectType(StartMode.class)
                    .defaultValue(StartMode.GROUP_OFFSETS)
                    .withDescription("init consumer position");

    public static final Option<Long> KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS =
            Options.key("partition-discovery.interval-millis")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("The interval for dynamically discovering topics and partitions.");
}

