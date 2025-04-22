package org.apache.seatunnel.connectors.seatunnel.tdengine.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class TDengineSinkOptions {

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The TDengine server URL, format: jdbc:TAOS-RS://host:port");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username for TDengine authentication");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password for TDengine authentication");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The TDengine database name");

    public static final Option<String> STABLE =
            Options.key("stable")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The TDengine super table name");

    public static final Option<String> TIMEZONE =
            Options.key("timezone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("The timezone used for timestamp conversion, default is UTC");
}
