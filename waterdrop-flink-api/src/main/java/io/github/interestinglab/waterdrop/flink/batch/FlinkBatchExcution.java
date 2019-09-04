package io.github.interestinglab.waterdrop.flink.batch;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.env.Execution;
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
public class FlinkBatchExcution implements Execution<FlinkBatchSource,AbstractFlinkBatchTransform,FlinkBatchSink> {

    private Config config;

    private FlinkBatchEnvironment flinkBatchEnvironment;

    private String jobName;

    public FlinkBatchExcution(FlinkBatchEnvironment flinkBatchEnvironment){
        this.flinkBatchEnvironment = flinkBatchEnvironment;
    }

    @Override
    public void start(List<FlinkBatchSource> sources, List<AbstractFlinkBatchTransform> transforms, List<FlinkBatchSink> sinks) {
        List<DataSet> data = new ArrayList<>();

        for (FlinkBatchSource source : sources) {
            data.add(source.getData(flinkBatchEnvironment));
        }

        DataSet input = data.get(0);

        for (AbstractFlinkBatchTransform transform : transforms) {
            input = transform.process(input, flinkBatchEnvironment);
        }

        for (FlinkBatchSink sink : sinks) {
            sink.output(input, flinkBatchEnvironment);
        }
        try {
            if (StringUtils.isBlank(jobName)){
                flinkBatchEnvironment.getEnvironment().execute();
            }else {
                flinkBatchEnvironment.getEnvironment().execute(jobName);
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
        return new CheckResult(true,"");
    }

    @Override
    public void prepare() {
        if (config.hasPath("job.name")){
            jobName = config.getString("job.name");
        }
    }
}
