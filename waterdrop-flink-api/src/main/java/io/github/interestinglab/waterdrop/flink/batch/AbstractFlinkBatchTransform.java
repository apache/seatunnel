package io.github.interestinglab.waterdrop.flink.batch;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import org.apache.flink.api.java.DataSet;

/**
 * @author mr_xiong
 * @date 2019-08-24 16:23
 * @description
 */
public abstract class AbstractFlinkBatchTransform<IN,OUT> implements BaseTransform<DataSet<IN>,DataSet<OUT>,FlinkBatchEnvironment> {

    protected Config config;

    @Override
    public abstract DataSet<OUT> process(DataSet<IN> data, FlinkBatchEnvironment env);

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }
}
