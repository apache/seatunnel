package org.apache.seatunnel.connectors.seatunnel.socket.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.io.Serializable;

@Data
public class SinkConfig implements Serializable {
    public static final  String HOST = "host";
    public static final  String PORT = "port";
    private static final  String MAX_RETRIES = "max_retries";
    private static final  int DEFAULT_MAX_RETRIES = 3;
    private String host;
    private Integer port;
    private Integer maxNumRetries = DEFAULT_MAX_RETRIES;
    public SinkConfig(Config config) {
        this.host = config.getString(HOST);
        this.port = config.getInt(PORT);
        if (config.hasPath(MAX_RETRIES)) {
            this.maxNumRetries = config.getInt(MAX_RETRIES);
        }
    }
}
