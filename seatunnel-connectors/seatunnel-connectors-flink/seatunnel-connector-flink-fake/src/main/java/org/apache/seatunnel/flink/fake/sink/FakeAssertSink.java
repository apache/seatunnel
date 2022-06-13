package org.apache.seatunnel.flink.fake.sink;

import org.apache.seatunnel.flink.BaseFlinkSink;
import com.google.auto.service.AutoService;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;

@AutoService(BaseFlinkSink.class)
public class FakeAssertSink implements FlinkBatchSink, FlinkStreamSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(FakeAssertSink.class);
    private Config config;

    @Override
    public void outputBatch(FlinkEnvironment env, DataSet<Row> inDataSet) {

    }

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {

    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public void close() throws IOException {

    }

}
