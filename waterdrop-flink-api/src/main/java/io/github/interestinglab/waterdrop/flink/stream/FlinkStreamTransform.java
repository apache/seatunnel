package io.github.interestinglab.waterdrop.flink.stream;

import io.github.interestinglab.waterdrop.flink.BaseFlinkTransform;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface FlinkStreamTransform<IN, OUT> extends BaseFlinkTransform {

    DataStream<OUT> processStream(FlinkEnvironment env, DataStream<IN> dataStream);
}
