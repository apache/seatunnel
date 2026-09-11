package org.apache.seatunnel.connectors.seatunnel.linear.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

public class LinearSourceParameter extends HttpParameter {
    public void buildWithConfig(ReadonlyConfig pluginConfig, String apiKey) {
        super.buildWithConfig(pluginConfig);
        this.headers.put("Authorization", apiKey);
    }
}
