package org.apache.seatunnel.connectors.seatunnel.socket.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class SocketSinkOptions extends SocketCommonOptions {

    private static final int DEFAULT_MAX_RETRIES = 3;

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(DEFAULT_MAX_RETRIES)
                    .withDescription("default value is " + DEFAULT_MAX_RETRIES + ", max retries");
}
