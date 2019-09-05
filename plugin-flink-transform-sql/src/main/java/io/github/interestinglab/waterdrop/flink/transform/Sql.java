package io.github.interestinglab.waterdrop.flink.transform;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author mr_xiong
 * @date 2019-07-02 10:20
 * @description
 */
public class Sql implements FlinkStreamTransform<Void,Void> {

    private String tableName;

    private String sql;

    private Config config;

    @Override
    public DataStream<Void> processStream(DataStream<Void> dataStream, FlinkEnvironment env) {
        StreamTableEnvironment tableEnvironment = env.getTableEnvironment();
        Table query = tableEnvironment.sqlQuery(sql);
        tableEnvironment.registerTable(tableName,query);
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
        sql = config.getString("sql");
    }
}
