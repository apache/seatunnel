package io.github.interestinglab.waterdrop.flink.stream;

import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author mr_xiong
 * @date 2019-05-28 23:00
 * @description
 */
public interface FlinkStreamSource<T> extends BaseSource {

    DataStream<T> getData(FlinkEnvironment env);


}
