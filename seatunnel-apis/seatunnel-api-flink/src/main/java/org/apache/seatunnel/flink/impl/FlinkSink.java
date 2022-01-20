package org.apache.seatunnel.flink.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;

/**
 * @author jayce
 * @version 1.0
 * @date 2022/1/20 3:43 下午
 */
public interface FlinkSink extends BaseFlinkSink {
    DataStream<RowData> output(FlinkEnvironment env, DataStream<RowData> dataSet);
}
