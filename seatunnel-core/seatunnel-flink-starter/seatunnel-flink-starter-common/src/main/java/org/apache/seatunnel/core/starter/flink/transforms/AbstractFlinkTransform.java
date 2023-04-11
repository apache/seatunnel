package org.apache.seatunnel.core.starter.flink.transforms;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

public abstract class AbstractFlinkTransform implements FlinkTransform {

    protected abstract void setConfig(Config config);

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        setConfig(pluginConfig);
    }
}
