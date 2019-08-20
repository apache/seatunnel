package io.github.interestinglab.waterdrop.flink.batch;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;

/**
 * @author mr_xiong
 * @date 2019-08-12 23:06
 * @description
 */
public class FlinkBatchEnvironment implements RuntimeEnv {

    private Config config;

    private ExecutionEnvironment executionEnvironment;

    private BatchTableEnvironment batchTableEnvironment;

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
