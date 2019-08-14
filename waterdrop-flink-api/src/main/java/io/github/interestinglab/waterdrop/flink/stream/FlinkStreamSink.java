package io.github.interestinglab.waterdrop.flink.stream;

import io.github.interestinglab.waterdrop.apis.BaseSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public interface FlinkStreamSink<IN,OUT> extends BaseSink<DataStream<IN>, DataStreamSink<OUT>, FlinkStreamEnvironment> {
    @Override
    DataStreamSink<OUT> output(DataStream<IN> dataStream, FlinkStreamEnvironment env);
}
