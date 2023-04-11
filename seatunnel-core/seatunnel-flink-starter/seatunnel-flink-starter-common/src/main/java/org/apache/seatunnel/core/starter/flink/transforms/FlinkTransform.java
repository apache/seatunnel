package org.apache.seatunnel.core.starter.flink.transforms;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.seatunnel.api.transform.Transform;
import org.apache.seatunnel.core.starter.flink.execution.FlinkRuntimeEnvironment;

/**
 * 添加flink私有转换组件
 * author sun_hming
 */
public interface FlinkTransform extends Transform {

    DataStream<Row> processStream(FlinkRuntimeEnvironment env, DataStream<Row> dataStream) throws Exception;

    default void registerFunction(FlinkRuntimeEnvironment flinkEnvironment) {}
}
