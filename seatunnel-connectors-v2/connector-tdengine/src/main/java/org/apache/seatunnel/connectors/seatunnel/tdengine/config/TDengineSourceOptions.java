package org.apache.seatunnel.connectors.seatunnel.tdengine.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TDengineSourceOptions {

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
                    .withDescription("The super table name in TDengine");

    public static final Option<String> LOWER_BOUND =
            Options.key("lowerBound")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The lower bound for data query range");

    public static final Option<String> UPPER_BOUND =
            Options.key("upperBound")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The upper bound for data query range");
}
