package org.apache.seatunnel.connectors.seatunnel.socket.source;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class SocketSourceParameter {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9999;
    private String host;
    private Integer port;

    public String getHost() {
        return StringUtils.isBlank(host) ? DEFAULT_HOST : host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return Objects.isNull(port) ? DEFAULT_PORT : port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
}
