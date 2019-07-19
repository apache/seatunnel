package io.github.interestinglab.waterdrop.flink.stream.source;

import com.typesafe.config.Config;

/**
 * @author mr_xiong
 * @date 2019-05-31 17:07
 * @description
 */
public abstract class InternalFlinkStreamSource<T> implements FlinkStreamSource<T> {

    protected Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

}
