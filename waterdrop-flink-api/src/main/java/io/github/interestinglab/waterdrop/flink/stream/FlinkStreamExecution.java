package io.github.interestinglab.waterdrop.flink.stream;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mr_xiong
 * @date 2019-08-12 23:51
 * @description
 */
public class FlinkStreamExecution implements Execution<FlinkStreamSource, AbstractFlinkStreamTransform, FlinkStreamSink> {

    private Config config;

    private FlinkStreamEnvironment streamEnvironment;

    public FlinkStreamExecution(FlinkStreamEnvironment streamEnvironment) {
        this.streamEnvironment = streamEnvironment;
    }

    @Override
    public void start(List<FlinkStreamSource> sources, List<AbstractFlinkStreamTransform> transforms, List<FlinkStreamSink> sinks) {
        List<DataStream> data = new ArrayList<>();

        for (FlinkStreamSource source : sources) {
            data.add(source.getData(streamEnvironment));
        }

        DataStream input = data.get(0);

        for (AbstractFlinkStreamTransform transform : transforms) {
            input = transform.process(input, streamEnvironment);
        }

        for (FlinkStreamSink sink : sinks) {
            sink.output(input, streamEnvironment);
        }
        try {
            streamEnvironment.getEnvironment().execute();
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
        return null;
    }

    @Override
    public void prepare() {

    }
}
