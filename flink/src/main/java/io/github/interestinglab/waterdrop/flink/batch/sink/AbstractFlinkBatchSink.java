package io.github.interestinglab.waterdrop.flink.batch.sink;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchEnv;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.java.DataSet;

/**
 * @author mr_xiong
 * @date 2019-06-28 18:21
 * @description
 */
public abstract class AbstractFlinkBatchSink<T> implements BaseSink<DataSet<T>,Void, FlinkBatchEnv> {

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

    @Override
    public abstract Void output(DataSet<T> tDataSet, FlinkBatchEnv env);
}
