package org.apache.seatunnel.connectors.seatunnel.typesense.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class TypesenseConnectionConfig {

    public static final Option<List<String>> HOSTS =
            Options.key("hosts")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Typesense cluster http address, the format is host:port, allowing multiple hosts to be specified. Such as [\"host1:8018\", \"host2:8018\"]");

    public static final Option<String> APIKEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Typesense api key");

    public static final Option<String> protocol =
            Options.key("protocol")
                    .stringType()
                    .defaultValue("http")
                    .withDescription("Default is http , for Typesense Cloud use https");
}
