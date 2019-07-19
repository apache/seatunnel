package io.github.interestinglab.waterdrop.flink.stream.sink;

import com.typesafe.config.Config;

/**
 * @author mr_xiong
 * @date 2019-05-28 23:26
 * @description
 */
public abstract class InternalFlinkStreamSink<IN,OUT> implements FlinkStreamSink<IN,OUT> {

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
