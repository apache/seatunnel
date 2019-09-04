package io.github.interestinglab.waterdrop.flink.source;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSource;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author mr_xiong
 * @date 2019-08-24 17:15
 * @description
 */
public class FileSource implements FlinkBatchSource<Row> {

    private Config config;

    private String path;

    @Override
    public DataSet<Row> getData(FlinkBatchEnvironment env) {
        ExecutionEnvironment environment = env.getEnvironment();
        return environment.readTextFile(path)
                .map(new MapFunction<String, Row>() {
                    @Override
                    public Row map(String value) throws Exception {
                        return Row.of(value);
                    }
                });
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
        path = config.getString("file.path");
    }
}
