package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.flink.stream.AbstractFlinkStreamTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnvironment;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author mr_xiong
 * @date 2019-07-02 10:20
 * @description
 */
public class Sql extends AbstractFlinkStreamTransform<Void,Void> {

    private String tableName;

    private String sql;

    @Override
    public DataStream<Void> process(DataStream<Void> dataStream, FlinkStreamEnvironment env) {
        StreamTableEnvironment tableEnvironment = env.getTableEnvironment();
        Table query = tableEnvironment.sqlQuery(sql);
        tableEnvironment.registerTable(tableName,query);
        return null;
    }

    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare() {
        tableName = config.getString("table_name");
        sql = config.getString("sql");
    }
}
