package io.github.interestinglab.waterdrop.flink.sink;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnvironment;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * @author mr_xiong
 * @date 2019-05-29 14:37
 * @description
 */
public class ConsoleSink implements FlinkStreamSink<Row,Row> {

    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }


    @Override
    public DataStreamSink<Row> output(DataStream<Row> dataStream, FlinkStreamEnvironment env) {
        return dataStream.print();
    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true,"");
    }

    @Override
    public void prepare() {

    }
}
