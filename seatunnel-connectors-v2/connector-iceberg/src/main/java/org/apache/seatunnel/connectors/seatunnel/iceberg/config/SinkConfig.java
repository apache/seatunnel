package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import lombok.Getter;
import lombok.ToString;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

/**
 *
 *
 * @author mustard
 * @version 1.0
 * Create by 2023-07-05
 */
@Getter
@ToString
public class SinkConfig extends CommonConfig {
    private static final long serialVersionUID = -196561967575264253L;

    public static final Option<Integer> KEY_MAX_ROW =
            Options.key("max_row")
                    .intType()
                    .defaultValue(1)
                    .withDescription(" sink max row");

    private int maxRow = 1;

    private final int maxDataSize = 1;

    private final boolean partitionedFanoutEnabled = false;

    private final String fileFormat = "parquet";

    public SinkConfig(Config pluginConfig) {
        super(pluginConfig);
    }

    public static SinkConfig loadConfig(Config pluginConfig) {
        SinkConfig sinkConfig = new SinkConfig(pluginConfig);

        if (pluginConfig.hasPath(KEY_MAX_ROW.key())) {
            sinkConfig.maxRow = pluginConfig.getInt(KEY_MAX_ROW.key());
        }

        return sinkConfig;
    }
}
