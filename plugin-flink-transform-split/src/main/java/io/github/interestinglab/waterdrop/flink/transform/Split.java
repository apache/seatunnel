package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.List;


public class Split implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

    private Config config;

    private static final String SEPARATOR = "separator";
    private static final String FIELDS = "fields";

    private String separator = ",";

    private int num;

    private List<String> fields;

    private RowTypeInfo rowTypeInfo;

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        return data;
    }

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        return dataStream;
    }

    @Override
    public void registerFunction(FlinkEnvironment flinkEnvironment) {
        if (flinkEnvironment.isStreaming()){
            flinkEnvironment
                    .getStreamTableEnvironment()
                    .registerFunction("split",new ScalarSplit(rowTypeInfo,num,separator));
        }else {
            flinkEnvironment
                    .getBatchTableEnvironment()
                    .registerFunction("split",new ScalarSplit(rowTypeInfo,num,separator));
        }
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
        return CheckConfigUtil.check(config,FIELDS);
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        fields = config.getStringList(FIELDS);
        num = fields.size();
        if (config.hasPath(SEPARATOR)){
            separator = config.getString(SEPARATOR);
        }
        TypeInformation[] types = new  TypeInformation[fields.size()];
        for (int i = 0; i< types.length; i++){
            types[i] = Types.STRING();
        }
        rowTypeInfo = new RowTypeInfo(types,fields.toArray(new String[]{}));
    }


}
