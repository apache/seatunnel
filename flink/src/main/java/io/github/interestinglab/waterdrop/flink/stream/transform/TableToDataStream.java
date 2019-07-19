package io.github.interestinglab.waterdrop.flink.stream.transform;

import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author mr_xiong
 * @date 2019-07-12 18:55
 * @description
 */
public class TableToDataStream extends AbstractFlinkStreamTransform<Void, Row> {

    private String tableName;

    @Override
    public DataStream<Row> process(DataStream<Void> dataStream, FlinkStreamEnv env) {
        StreamTableEnvironment tableEnvironment = env.getTableEnvironment();
        Table table = tableEnvironment.scan(tableName);
        TypeInformation<?>[] informations = table.getSchema().getFieldTypes();
        String[] fieldNames = table.getSchema().getFieldNames();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(informations, fieldNames);
        SingleOutputStreamOperator<Row> ds = tableEnvironment
                .toRetractStream(table, rowTypeInfo)
                .filter(new FilterFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, Row> value) throws Exception {
                        return value.f0;
                    }
                })
                .map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
                    @Override
                    public Row map(Tuple2<Boolean, Row> value) throws Exception {
                        return value.f1;
                    }
                });
        return ds;
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
