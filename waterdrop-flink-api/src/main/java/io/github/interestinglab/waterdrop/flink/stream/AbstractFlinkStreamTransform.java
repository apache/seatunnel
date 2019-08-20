package io.github.interestinglab.waterdrop.flink.stream;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author mr_xiong
 * @date 2019-05-28 23:26
 * @description
 */
public abstract class AbstractFlinkStreamTransform<IN,OUT> implements BaseTransform<DataStream<IN>,DataStream<OUT>, FlinkStreamEnvironment> {

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
    public abstract DataStream<OUT> process(DataStream<IN> dataStream, FlinkStreamEnvironment env);
}
