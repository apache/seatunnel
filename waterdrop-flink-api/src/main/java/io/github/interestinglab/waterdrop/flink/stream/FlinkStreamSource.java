package io.github.interestinglab.waterdrop.flink.stream;

import io.github.interestinglab.waterdrop.apis.BaseSource;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author mr_xiong
 * @date 2019-05-28 23:00
 * @description
 */
public interface FlinkStreamSource<T> extends BaseSource<DataStream<T>, FlinkStreamEnvironment> {

    @Override
    DataStream<T> getData(FlinkStreamEnvironment env);
}
