package io.github.interestinglab.waterdrop.flink.sink;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

/**
 * @author mr_xiong
 * @date 2019-08-24 17:30
 * @description
 */
public class BatchConsoleSink extends RichOutputFormat<Row> implements FlinkBatchSink<Row, Row> {

    private Config config;

    @Override
    public DataSink<Row> output(DataSet<Row> rowDataSet, FlinkBatchEnvironment env) {

        return rowDataSet.output(this);
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
    public CheckResult checkConfig() {
        return new CheckResult(true, "");
    }

    @Override
    public void prepare() {

    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) {

    }

    @Override
    public void writeRecord(Row record) {
        System.out.println(record);
    }

    @Override
    public void close() {

    }
}
