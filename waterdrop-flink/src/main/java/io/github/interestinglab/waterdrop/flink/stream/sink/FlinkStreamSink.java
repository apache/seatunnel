package io.github.interestinglab.waterdrop.flink.stream.sink;

import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public interface FlinkStreamSink<IN,OUT> extends BaseSink<DataStream<IN>, DataStreamSink<OUT>, FlinkStreamEnv> {
    @Override
    DataStreamSink<OUT> output(DataStream<IN> dataStream, FlinkStreamEnv env);
}
