package io.github.interestinglab.waterdrop.flink.transform;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author mr_xiong
 * @date 2019-07-12 18:52
 * @description
 */
public class DataSteamToTable implements FlinkStreamTransform<Row,Void>, FlinkBatchTransform<Row,Void> {

    private String tableName;

    private Config config;

    @Override
    public DataStream<Void> processStream(DataStream<Row> dataStream, FlinkEnvironment env) {
        StreamTableEnvironment tableEnvironment = env.getTableEnvironment();
        tableEnvironment.registerDataStream(tableName,dataStream);
        return null;
    }

    @Override
    public DataSet<Void> processBatch(DataSet<Row> data, FlinkEnvironment env) {
        env.getBatchTableEnvironment().registerDataSet(tableName,data);
        return null;
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
        return null;
    }

    @Override
    public void prepare() {
        tableName = config.getString("table_name");
    }
}
