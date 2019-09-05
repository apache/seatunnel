package io.github.interestinglab.waterdrop.flink.stream;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author mr_xiong
 * @date 2019-05-28 23:26
 * @description
 */
public interface FlinkStreamTransform<IN,OUT> extends BaseTransform {

    DataStream<OUT> processStream(DataStream<IN> dataStream, FlinkEnvironment env);
}
