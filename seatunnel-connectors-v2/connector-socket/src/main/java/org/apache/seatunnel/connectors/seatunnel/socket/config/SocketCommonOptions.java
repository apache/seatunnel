package org.apache.seatunnel.connectors.seatunnel.socket.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class SocketCommonOptions {

    public static final String identifier = "Socket";

    public static final Option<String> HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("socket host");

    public static final Option<Integer> PORT =
            Options.key("port").intType().noDefaultValue().withDescription("socket port");
}
