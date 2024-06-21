package org.apache.seatunnel.connectors.seatunnel.influxdb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class MultipleTableSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @Getter private List<SourceConfig> sourceConfigs;

    public MultipleTableSourceConfig(ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.getOptional(SourceConfig.TABLE_CONFIGS).isPresent()) {
            parseSourceConfigs(readonlyConfig);
        } else {
            parseSourceConfig(readonlyConfig);
        }
    }

    private void parseSourceConfigs(ReadonlyConfig readonlyConfig) {
        this.sourceConfigs =
                readonlyConfig.get(SourceConfig.TABLE_CONFIGS).stream()
                        .map(ReadonlyConfig::fromMap)
                        .map(SourceConfig::loadConfig)
                        .collect(Collectors.toList());
    }

    private void parseSourceConfig(ReadonlyConfig localFileSourceRootConfig) {
        SourceConfig influxdbSourceConfig = SourceConfig.loadConfig(localFileSourceRootConfig);
        this.sourceConfigs = Lists.newArrayList(influxdbSourceConfig);
    }
}
