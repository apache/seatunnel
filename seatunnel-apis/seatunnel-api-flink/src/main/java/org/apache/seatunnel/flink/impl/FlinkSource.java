package org.apache.seatunnel.flink.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.data.RowData;
import org.apache.seatunnel.flink.BaseFlinkSource;
import org.apache.seatunnel.flink.FlinkEnvironment;

/**
 * @author jayce
 * @version 1.0
 * @date 2022/1/20 3:42 下午
 */
public interface FlinkSource extends BaseFlinkSource {
    DataStream<RowData> getData(FlinkEnvironment env);
}

