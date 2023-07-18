package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import lombok.Getter;
import lombok.ToString;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

@Getter
@ToString
public class SinkConfig extends CommonConfig {
    private static final long serialVersionUID = -196561967575264253L;

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("batch size");

    private int batchSize;

    private int batchIntervalMs;

    private final int maxDataSize = 1;

    private final boolean partitionedFanoutEnabled = false;

    private final String fileFormat = "parquet";

    public SinkConfig(Config pluginConfig) {
        super(pluginConfig);
    }

    public static SinkConfig loadConfig(Config pluginConfig) {
        SinkConfig sinkConfig = new SinkConfig(pluginConfig);

        if (pluginConfig.hasPath(BATCH_SIZE.key())) {
            sinkConfig.batchSize = pluginConfig.getInt(BATCH_SIZE.key());
        }

        return sinkConfig;
    }
}
