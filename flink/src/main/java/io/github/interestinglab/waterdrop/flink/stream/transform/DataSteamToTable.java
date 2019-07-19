package io.github.interestinglab.waterdrop.flink.stream.transform;

import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamEnv;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author mr_xiong
 * @date 2019-07-12 18:52
 * @description
 */
public class DataSteamToTable extends AbstractFlinkStreamTransform<Row,Void> {

    private String tableName;

    @Override
    public DataStream<Void> process(DataStream<Row> dataStream, FlinkStreamEnv env) {
        StreamTableEnvironment tableEnvironment = env.getTableEnvironment();
        //===========窗口操作需要+wtime.rowtime
//        String[] names = ((RowTypeInfo) dataStream.getType()).getFieldNames();
//        String join = StringUtils.join(names, ",");
//        tableEnvironment.registerDataStream(tableName,dataStream,"wtime.rowtime,"+join);
        //===========
        tableEnvironment.registerDataStream(tableName,dataStream);
        return null;
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
