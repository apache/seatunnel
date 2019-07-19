package io.github.interestinglab.waterdrop.flink.stream;


import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.flink.stream.sink.FlinkStreamSink;
import io.github.interestinglab.waterdrop.flink.stream.source.FlinkStreamSource;
import io.github.interestinglab.waterdrop.flink.stream.transform.AbstractFlinkStreamTransform;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mr_xiong
 * @date 2019-05-28 16:31
 * @description
 */
public class FlinkStreamEnv implements RuntimeEnv<FlinkStreamSource, AbstractFlinkStreamTransform, FlinkStreamSink> {

    private Config config;

    private StreamExecutionEnvironment environment;

    private StreamTableEnvironment tableEnvironment;


    @Override
    public void start(List<FlinkStreamSource> sources
            , List<AbstractFlinkStreamTransform> transforms
            , List<FlinkStreamSink> sinks) {
        List<DataStream> data = new ArrayList<>();

        for (FlinkStreamSource source : sources) {
            data.add(source.getData(this));
        }

        DataStream input = data.get(0);

        for (AbstractFlinkStreamTransform transform : transforms) {
            input = transform.process(input, this);
        }

        for (FlinkStreamSink sink : sinks) {
            sink.output(input, this);
        }
        try {
            environment.execute();
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
    }

    public StreamExecutionEnvironment getEnvironment() {
        if (environment == null) {
            createEnvironment();
        }
        return environment;
    }

    public StreamTableEnvironment getTableEnvironment() {
        if (tableEnvironment == null) {
            tableEnvironment = StreamTableEnvironment.create(getEnvironment());
        }
        return tableEnvironment;
    }

    private void createEnvironment() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo com.qsc.bigdata.config 配置
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }
}
