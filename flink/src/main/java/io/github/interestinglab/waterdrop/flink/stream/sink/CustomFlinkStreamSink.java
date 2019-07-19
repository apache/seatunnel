package io.github.interestinglab.waterdrop.flink.stream.sink;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author mr_xiong
 * @date 2019-07-04 16:47
 * @description
 */
public abstract class CustomFlinkStreamSink<IN,OUT> extends RichSinkFunction<IN> implements FlinkStreamSink<IN,OUT> {

    protected Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare() {

    }
}
