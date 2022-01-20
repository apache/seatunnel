package org.apache.seatunnel.flink.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.seatunnel.flink.BaseFlinkTransform;
import org.apache.seatunnel.flink.FlinkEnvironment;

/**
 * @author jayce
 * @version 1.0
 * @date 2022/1/20 3:44 下午
 */
public interface FlinkTransform extends BaseFlinkTransform {
    DataStream<RowData> process(FlinkEnvironment env, DataStream<RowData> data);
}