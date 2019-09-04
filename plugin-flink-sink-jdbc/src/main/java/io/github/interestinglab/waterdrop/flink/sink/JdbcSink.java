package io.github.interestinglab.waterdrop.flink.sink;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.common.PropertiesUtil;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnvironment;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;

/**
 * @author mr_xiong
 * @date 2019-08-31 16:40
 * @description
 */
public class JdbcSink implements FlinkStreamSink<Void,Void> {

    private Config config;
    private String tableName;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private int batchSize;

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
        tableName = config.getString("table_name");
    }

    @Override
    public DataStreamSink<Void> output(DataStream<Void> dataStream, FlinkStreamEnvironment env) {
        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setUsername(username)
                .setPassword(password)
                .setBatchSize(batchSize)
                .setQuery("INSERT INTO books (id) VALUES (?)")
                .setParameterTypes(INT_TYPE_INFO)
                .build();
        env.getTableEnvironment().registerTableSink("jdbc_table",sink);
        env.getTableEnvironment().scan(tableName).insertInto("jdbc_table");
        return null;
    }
}
