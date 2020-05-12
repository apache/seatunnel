package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import io.github.interestinglab.waterdrop.flink.util.TableUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Sql implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

    private String sql;

    private Config config;

    private static final String SQL = "sql";

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = tableEnvironment.sqlQuery(sql);
        return TableUtil.tableToDataStream(tableEnvironment, table, false);
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        BatchTableEnvironment tableEnvironment = env.getBatchTableEnvironment();
        Table table = tableEnvironment.sqlQuery(sql);
        return TableUtil.tableToDataSet(tableEnvironment, table);
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
       return CheckConfigUtil.check(config,SQL);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        sql = config.getString("sql");
    }
}
