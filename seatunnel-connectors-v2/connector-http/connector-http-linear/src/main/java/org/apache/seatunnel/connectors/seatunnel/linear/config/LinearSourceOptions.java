package org.apache.seatunnel.connectors.seatunnel.linear.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpOption;

public class LinearSourceOptions extends HttpOption {
    public static final Option<String> API_KEY =
            Options.key("api_key").stringType().noDefaultValue().withDescription("Linear API Key");
}
