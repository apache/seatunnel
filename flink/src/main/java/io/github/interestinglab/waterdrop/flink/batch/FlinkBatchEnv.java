package io.github.interestinglab.waterdrop.flink.batch;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.flink.batch.sink.AbstractFlinkBatchSink;
import io.github.interestinglab.waterdrop.flink.batch.source.AbstractFlinkBatchSource;
import io.github.interestinglab.waterdrop.flink.batch.transform.AbstractFlinkBatchTransform;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mr_xiong
 * @date 2019-06-28 18:18
 * @description
 */
public class FlinkBatchEnv implements RuntimeEnv<AbstractFlinkBatchSource, AbstractFlinkBatchTransform, AbstractFlinkBatchSink> {

    private Config config;

    private ExecutionEnvironment environment;

    private BatchTableEnvironment tableEnvironment;

    @Override
    public void start(List<AbstractFlinkBatchSource> sources, List<AbstractFlinkBatchTransform> transforms, List<AbstractFlinkBatchSink> sinks) {

        List<DataSet> data = new ArrayList<>();

        for (AbstractFlinkBatchSource source : sources) {
            data.add(source.getData(this));
        }

        DataSet input = data.get(0);

        for (AbstractFlinkBatchTransform transform : transforms) {
            input = transform.process(input, this);
        }

        for (AbstractFlinkBatchSink sink : sinks) {
            sink.output(input, this);
        }
    }

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

    public ExecutionEnvironment getEnvironment() {
        if (environment == null) {
            environment = ExecutionEnvironment.getExecutionEnvironment();
        }
        return environment;
    }

    public BatchTableEnvironment getTableEnvironment() {
        if (tableEnvironment == null){
            tableEnvironment = BatchTableEnvironment.create(getEnvironment());
        }
        return tableEnvironment;
    }
}
