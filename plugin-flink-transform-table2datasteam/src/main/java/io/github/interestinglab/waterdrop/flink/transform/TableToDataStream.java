package io.github.interestinglab.waterdrop.flink.transform;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author mr_xiong
 * @date 2019-07-12 18:55
 * @description
 */
public class TableToDataStream implements FlinkStreamTransform<Void, Row>, FlinkBatchTransform<Void, Row> {

    private String tableName;

    private Config config;

    @Override
    public DataStream<Row> processStream(DataStream<Void> dataStream, FlinkEnvironment env) {
        StreamTableEnvironment tableEnvironment = env.getTableEnvironment();
        Table table = tableEnvironment.scan(tableName);
        TypeInformation<?>[] informations = table.getSchema().getFieldTypes();
        String[] fieldNames = table.getSchema().getFieldNames();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(informations, fieldNames);
        SingleOutputStreamOperator<Row> ds = tableEnvironment
                .toRetractStream(table, rowTypeInfo)
                .filter(row -> row.f0)
                .map(row -> row.f1)
                .returns(rowTypeInfo);
        return ds;
    }

    @Override
    public DataSet<Row> processBatch(DataSet<Void> data, FlinkEnvironment env) {

        BatchTableEnvironment batchTableEnvironment = env.getBatchTableEnvironment();
        Table table = batchTableEnvironment.scan(tableName);
        TypeInformation<?>[] fieldTypes = table.getSchema().getFieldTypes();
        String[] fieldNames = table.getSchema().getFieldNames();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        return batchTableEnvironment.toDataSet(table,rowTypeInfo);
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
