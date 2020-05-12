package io.github.interestinglab.waterdrop.flink.stream;

import io.github.interestinglab.waterdrop.flink.BaseFlinkSource;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface FlinkStreamSource<T> extends BaseFlinkSource {

    DataStream<T> getData(FlinkEnvironment env);


}
