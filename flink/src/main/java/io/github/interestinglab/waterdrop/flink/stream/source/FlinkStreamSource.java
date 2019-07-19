package io.github.interestinglab.waterdrop.flink.stream.source;

import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author mr_xiong
 * @date 2019-05-28 23:00
 * @description
 */
public interface FlinkStreamSource<T> extends BaseSource<DataStream<T>, FlinkStreamEnv> {

    @Override
    DataStream<T> getData(FlinkStreamEnv env);
}
