package io.github.interestinglab.waterdrop.flink.transform;

import com.typesafe.config.waterdrop.Config;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author mr_xiong
 * @date 2020-01-14 16:37
 * @description
 */
public class Split implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

    private Config config;

    private String separator = ",";

    private int num;

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
    public List<TableFunction<Row>> getTableFunction(FlinkEnvironment flinkEnvironment) {

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
        return new CheckResult(true,"");
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        num = config.getInt("num");
        TypeInformation[] types = new  TypeInformation[num];
        for (int i = 0; i< types.length; i++){
            types[i] = Types.STRING();
        }
    }

    class SplitFunction extends TableFunction<Row> {
        public void eval(String str) {
            Row row = new Row(num);
            int i = 0;
            for (String s : str.split(separator,num)) {
                row.setField(i++, s);
                collect(row);
            }
        }

        @Override
        public TypeInformation<Row> getResultType() {
            return rowTypeInfo;
        }
    }
}
