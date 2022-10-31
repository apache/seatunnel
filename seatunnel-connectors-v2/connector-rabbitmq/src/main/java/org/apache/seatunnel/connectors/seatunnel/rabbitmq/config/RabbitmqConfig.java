package org.apache.seatunnel.connectors.seatunnel.rabbitmq.config;

import org.apache.seatunnel.common.config.TypesafeConfigUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
@AllArgsConstructor
public class RabbitmqConfig implements Serializable {
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String VIRTUAL_HOST = "virtual_host";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String URL = "url";
    public static final String NETWORK_RECOVERY_INTERVAL = "network_recovery_interval";
    public static final String AUTOMATIC_RECOVERY = "automatic_recovery";
    public static final String TOPOLOGY_RECOVERY = "topology_recovery";
    public static final String CONNECTION_TIMEOUT = "connection_timeout";
    public static final String REQUESTED_CHANNEL_MAX = "requested_channel_max";
    public static final String REQUESTED_FRAME_MAX = "requested_frame_max";
    public static final String REQUESTED_HEARTBEAT = "requested_heartbeat";

    public static final String PREFETCH_COUNT = "prefetch_count";
    public static final String DELIVERY_TIMEOUT = "delivery_timeout";
    public static final String QUEUE_NAME = "queue_name";
    public static final String ROUTING_KEY = "routing_key";
    public static final String EXCHANGE = "exchange";


    public static final String LOG_FAILURES_ONLY = "log_failures_only";

    private String host;
    private Integer port;
    private String virtualHost;
    private String username;
    private String password;
    private String uri;
    private Integer networkRecoveryInterval;
    private Boolean automaticRecovery;
    private Boolean topologyRecovery;
    private Integer connectionTimeout;
    private Integer requestedChannelMax;
    private Integer requestedFrameMax;
    private Integer requestedHeartbeat;
    private Integer prefetchCount;
    private  long deliveryTimeout;
    private String queueName;
    private String routingKey;
    private boolean logFailuresOnly = false;
    private String exchange = "";
    public static final String RABBITMQ_SINK_CONFIG_PREFIX = "rabbitmq.properties.";

    private final Map<String, Object> sinkOptionProps = new HashMap<>();

    private void parseSinkOptionProperties(Config pluginConfig) {
        Config sinkOptionConfig = TypesafeConfigUtils.extractSubConfig(pluginConfig,
                RABBITMQ_SINK_CONFIG_PREFIX, false);
        sinkOptionConfig.entrySet().forEach(entry -> {
            final String configKey = entry.getKey().toLowerCase();
            this.sinkOptionProps.put(configKey, entry.getValue().unwrapped());
        });
    }

    public RabbitmqConfig(Config config) {
        this.host = config.getString(HOST);
        this.port = config.getInt(PORT);
        this.queueName = config.getString(QUEUE_NAME);
        if (config.hasPath(USERNAME)) {
            this.username = config.getString(USERNAME);
        }
        if (config.hasPath(PASSWORD)) {
            this.password = config.getString(PASSWORD);
        }
        if (config.hasPath(VIRTUAL_HOST)) {
            this.virtualHost = config.getString(VIRTUAL_HOST);
        }
        if (config.hasPath(NETWORK_RECOVERY_INTERVAL)) {
            this.networkRecoveryInterval = config.getInt(NETWORK_RECOVERY_INTERVAL);
        }
        if (config.hasPath(AUTOMATIC_RECOVERY)) {
            this.automaticRecovery = config.getBoolean(AUTOMATIC_RECOVERY);
        }
        if (config.hasPath(CONNECTION_TIMEOUT)) {
            this.connectionTimeout = config.getInt(CONNECTION_TIMEOUT);
        }
        if (config.hasPath(REQUESTED_CHANNEL_MAX)) {
            this.requestedChannelMax = config.getInt(REQUESTED_CHANNEL_MAX);
        }
        if (config.hasPath(REQUESTED_FRAME_MAX)) {
            this.requestedFrameMax = config.getInt(REQUESTED_FRAME_MAX);
        }
        if (config.hasPath(REQUESTED_HEARTBEAT)) {
            this.requestedHeartbeat = config.getInt(REQUESTED_HEARTBEAT);
        }
        if (config.hasPath(PREFETCH_COUNT)) {
            this.prefetchCount = config.getInt(PREFETCH_COUNT);
        }
        if (config.hasPath(DELIVERY_TIMEOUT)) {
            this.deliveryTimeout = config.getInt(DELIVERY_TIMEOUT);
        }
        if (config.hasPath(ROUTING_KEY)) {
            this.routingKey = config.getString(ROUTING_KEY);
        }
        if (config.hasPath(EXCHANGE)) {
            this.exchange = config.getString(EXCHANGE);
        }
        parseSinkOptionProperties(config);

    }
}
