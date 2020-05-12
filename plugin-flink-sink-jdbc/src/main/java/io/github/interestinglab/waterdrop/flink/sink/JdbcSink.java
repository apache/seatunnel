package io.github.interestinglab.waterdrop.flink.sink;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import io.github.interestinglab.waterdrop.flink.util.SchemaUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;


public class JdbcSink implements FlinkStreamSink<Row, Row>, FlinkBatchSink<Row, Row> {

    private Config config;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private String query;
    private int batchSize = 5000;

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
        return CheckConfigUtil.check(config,"driver","url","username","query");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        driverName = config.getString("driver");
        dbUrl = config.getString("url");
        username = config.getString("username");
        query = config.getString("query");
        if (config.hasPath("password")) {
            password = config.getString("password");
        }
        if (config.hasPath("batch_size")) {
            batchSize = config.getInt("batch_size");
        }
    }


    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        Table table = env.getStreamTableEnvironment().fromDataStream(dataStream);
        createSink(env.getStreamTableEnvironment(),table);
        return null;
    }

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        final Table table = env.getBatchTableEnvironment().fromDataSet(dataSet);
        createSink(env.getBatchTableEnvironment(),table);
        return null;
    }

    private void createSink(TableEnvironment tableEnvironment,Table table) {
        TypeInformation<?>[] fieldTypes = table.getSchema().getFieldTypes();
        String[] fieldNames = table.getSchema().getFieldNames();
        TableSink sink = JDBCAppendTableSink.builder()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setUsername(username)
                .setPassword(password)
                .setBatchSize(batchSize)
                .setQuery(query)
                .setParameterTypes(fieldTypes)
                .build()
                .configure(fieldNames,fieldTypes);
        String uniqueTableName = SchemaUtil.getUniqueTableName();
        tableEnvironment.registerTableSink(uniqueTableName,sink);
        table.insertInto(uniqueTableName);
    }
}
