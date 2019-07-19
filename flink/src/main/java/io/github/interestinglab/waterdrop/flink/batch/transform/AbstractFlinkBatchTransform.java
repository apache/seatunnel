package io.github.interestinglab.waterdrop.flink.batch.transform;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchEnv;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.java.DataSet;


/**
 * @author mr_xiong
 * @date 2019-06-28 18:23
 * @description
 */
public abstract class AbstractFlinkBatchTransform<T> implements BaseTransform<DataSet<T>,DataSet<T>, FlinkBatchEnv> {

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
    public abstract DataSet<T> process(DataSet<T> dataSets, FlinkBatchEnv env);
}
