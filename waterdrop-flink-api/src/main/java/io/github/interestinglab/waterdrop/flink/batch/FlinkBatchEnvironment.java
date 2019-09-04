package io.github.interestinglab.waterdrop.flink.batch;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.flink.util.ConfigKeyName;
import io.github.interestinglab.waterdrop.flink.util.EnvironmentUtil;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @author mr_xiong
 * @date 2019-08-12 23:06
 * @description
 */
public class FlinkBatchEnvironment implements RuntimeEnv {

    private Config config;

    private ExecutionEnvironment environment;

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
        return EnvironmentUtil.checkRestartStrategy(config);
    }

    @Override
    public void prepare() {
        createBatchTableEnvironment();
        createExecutionEnvironment();
    }

    public ExecutionEnvironment getEnvironment() {
        return environment;
    }

    public BatchTableEnvironment getBatchTableEnvironment() {
        return batchTableEnvironment;
    }

    private void createExecutionEnvironment() {
        environment = ExecutionEnvironment.createCollectionsEnvironment();
        if (config.hasPath(ConfigKeyName.PARALLELISM)) {
            int parallelism = config.getInt(ConfigKeyName.PARALLELISM);
            environment.setParallelism(parallelism);
        }
        EnvironmentUtil.setRestartStrategy(config, environment);
    }

    private void createBatchTableEnvironment() {
        batchTableEnvironment = BatchTableEnvironment.create(environment);
    }
}
