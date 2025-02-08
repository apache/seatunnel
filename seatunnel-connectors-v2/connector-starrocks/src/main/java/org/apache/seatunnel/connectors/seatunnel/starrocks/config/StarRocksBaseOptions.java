package org.apache.seatunnel.connectors.seatunnel.starrocks.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;
import java.util.List;

public class StarRocksBaseOptions implements Serializable {
    public static final String CONNECTOR_IDENTITY = "StarRocks";
    public static final Option<List<String>> NODE_URLS =
            Options.key("nodeUrls")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "StarRocks cluster address, the format is [\"fe_ip:fe_http_port\", ...]");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of StarRocks database");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of StarRocks table");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("StarRocks user username");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("StarRocks user password");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of retries to flush failed");
}
