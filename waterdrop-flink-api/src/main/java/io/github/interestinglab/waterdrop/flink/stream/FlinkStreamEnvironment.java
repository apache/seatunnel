package io.github.interestinglab.waterdrop.flink.stream;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author mr_xiong
 * @date 2019-08-12 22:58
 * @description
 */
public class FlinkStreamEnvironment implements RuntimeEnv {

    private Config config;

    private StreamExecutionEnvironment environment;

    private StreamTableEnvironment tableEnvironment;

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
        createEnvironment();
        createStreamTableEnvironment();
    }

    public StreamExecutionEnvironment getEnvironment() {
        return environment;
    }

    public StreamTableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }

    private void createStreamTableEnvironment(){
        tableEnvironment = StreamTableEnvironment.create(getEnvironment());
    }

    private void createEnvironment() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo com.qsc.bigdata.config 配置
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }
}
