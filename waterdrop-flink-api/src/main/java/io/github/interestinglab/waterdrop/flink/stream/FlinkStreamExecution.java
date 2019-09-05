package io.github.interestinglab.waterdrop.flink.stream;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mr_xiong
 * @date 2019-08-12 23:51
 * @description
 */
public class FlinkStreamExecution implements Execution<FlinkStreamSource, FlinkStreamTransform, FlinkStreamSink> {

    private Config config;

    private FlinkEnvironment streamEnvironment;

    private String jobName;

    public FlinkStreamExecution(FlinkEnvironment streamEnvironment) {
        this.streamEnvironment = streamEnvironment;
    }

    @Override
    public void start(List<FlinkStreamSource> sources, List<FlinkStreamTransform> transforms, List<FlinkStreamSink> sinks) {
        List<DataStream> data = new ArrayList<>();

        for (FlinkStreamSource source : sources) {
            data.add(source.getData(streamEnvironment));
        }

        DataStream input = data.get(0);

        for (FlinkStreamTransform transform : transforms) {
            input = transform.processStream(input, streamEnvironment);
        }

        for (FlinkStreamSink sink : sinks) {
            sink.outputStream(streamEnvironment,input);
        }
        try {
            if (StringUtils.isBlank(jobName)){
                streamEnvironment.getEnvironment().execute();
            }else {
                streamEnvironment.getEnvironment().execute(jobName);
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
