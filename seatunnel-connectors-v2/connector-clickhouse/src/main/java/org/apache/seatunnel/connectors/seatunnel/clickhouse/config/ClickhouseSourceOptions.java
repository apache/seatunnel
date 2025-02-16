package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class ClickhouseSourceOptions {

    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse sql used to query data");
}
