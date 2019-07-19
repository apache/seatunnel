package io.github.interestinglab.waterdrop.flink.stream.sink;

import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;


/**
 * @author mr_xiong
 * @date 2019-05-29 14:37
 * @description
 */
public class ConsoleSink extends InternalFlinkStreamSink<Row,Row> {
    @Override
    public DataStreamSink<Row> output(DataStream<Row> dataStream, FlinkStreamEnv env) {
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
