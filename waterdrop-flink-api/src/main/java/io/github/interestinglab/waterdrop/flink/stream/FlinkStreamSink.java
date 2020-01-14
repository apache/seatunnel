package io.github.interestinglab.waterdrop.flink.stream;

import io.github.interestinglab.waterdrop.flink.BaseFlinkSink;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public interface FlinkStreamSink<IN, OUT> extends BaseFlinkSink {

    DataStreamSink<OUT> outputStream(FlinkEnvironment env, DataStream<IN> dataStream);

}
