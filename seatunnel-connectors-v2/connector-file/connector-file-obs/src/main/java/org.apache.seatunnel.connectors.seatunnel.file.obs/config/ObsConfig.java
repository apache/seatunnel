package org.apache.seatunnel.connectors.seatunnel.obs.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;

public class ObsConfig extends BaseSourceConfig {

    public static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OBS bucket access key");
    public static final Option<String> SECURITY_KEY =
            Options.key("security_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OBS bucket security key");
    public static final Option<String> ENDPOINT =
            Options.key("endpoint").stringType().noDefaultValue().withDescription("OBS endpoint");
    public static final Option<String> BUCKET =
            Options.key("bucket").stringType().noDefaultValue().withDescription("OBS bucket");
}
