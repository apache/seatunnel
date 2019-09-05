package io.github.interestinglab.waterdrop.flink.batch;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mr_xiong
 * @date 2019-08-24 16:26
 * @description
 */
public class FlinkBatchExecution implements Execution<FlinkBatchSource, FlinkBatchTransform, FlinkBatchSink> {

    private Config config;

    private FlinkEnvironment flinkEnvironment;

    private String jobName;

    public FlinkBatchExecution(FlinkEnvironment flinkEnvironment) {
        this.flinkEnvironment = flinkEnvironment;
    }

    @Override
    public void start(List<FlinkBatchSource> sources, List<FlinkBatchTransform> transforms, List<FlinkBatchSink> sinks) {
        List<DataSet> data = new ArrayList<>();

        for (FlinkBatchSource source : sources) {
            data.add(source.getData(flinkEnvironment));
        }

        DataSet input = data.get(0);

        for (FlinkBatchTransform transform : transforms) {
            input = transform.processBatch(input, flinkEnvironment);
        }

        for (FlinkBatchSink sink : sinks) {
            sink.outputBatch(input, flinkEnvironment);
        }
        try {
            if (StringUtils.isBlank(jobName)) {
                flinkEnvironment.getBatchEnvironment().execute();
            } else {
                flinkEnvironment.getBatchEnvironment().execute(jobName);
            }
        } catch (Exception e) {
            e.printStackTrace();
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
        return new CheckResult(true, "");
    }

    @Override
    public void prepare() {
        if (config.hasPath("job.name")) {
            jobName = config.getString("job.name");
        }
    }
}
