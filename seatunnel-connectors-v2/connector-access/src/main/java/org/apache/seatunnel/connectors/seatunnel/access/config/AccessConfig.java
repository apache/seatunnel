package org.apache.seatunnel.connectors.seatunnel.access.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class AccessConfig {
    public static final Integer DEFAULT_BATCH_SIZE = 5000;

    public static final Option<String> DRIVER =
            Options.key("driver").stringType().noDefaultValue().withDescription("driver");

    public static final Option<String> URL =
            Options.key("url").stringType().noDefaultValue().withDescription("url");

    public static final Option<String> USERNAME =
            Options.key("username").stringType().noDefaultValue().withDescription("username");

    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("password");

    public static final Option<String> QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("query");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("table");

    public static final Option<String> FIELDS =
            Options.key("fields").stringType().defaultValue("LOCAL_ONE").withDescription("fields");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(DEFAULT_BATCH_SIZE)
                    .withDescription("");
}
