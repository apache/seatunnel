package org.apache.seatunnel.flink.transform;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchTransform;
import org.apache.seatunnel.flink.stream.FlinkStreamTransform;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

public class Replace implements FlinkStreamTransform, FlinkBatchTransform {

    private Config config;

    private static final String PATTERN = "pattern";
    private static final String REPLACEMENT = "replacement";
    private static final String ISREGEX = "is_regex";
    private static final String REPLACEFIRST = "replace_first";

    private String pattern = "";
    private String replacement = "";
    private Boolean isRegex = false;
    private Boolean replaceFirst = false;


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
        return CheckConfigUtil.checkAllExists(config);
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {

        if (config.hasPath(PATTERN)) {
            pattern = config.getString(PATTERN);
        }
        if (config.hasPath(REPLACEMENT)) {
            replacement = config.getString(REPLACEMENT);
        }
        if (config.hasPath(ISREGEX)) {
            isRegex = config.getBoolean(ISREGEX);
        }
        if (config.hasPath(REPLACEFIRST)) {
            replaceFirst = config.getBoolean(REPLACEFIRST);
        }
    }

    @Override
    public void registerFunction(FlinkEnvironment flinkEnvironment) {
        FlinkStreamTransform.super.registerFunction(flinkEnvironment);

        if (flinkEnvironment.isStreaming()) {
            flinkEnvironment.getStreamTableEnvironment().registerFunction("replace", new ScalarReplace(pattern, replacement, isRegex, replaceFirst));
        } else {
            flinkEnvironment.getBatchTableEnvironment().registerFunction("replace", new ScalarReplace(pattern, replacement, isRegex, replaceFirst));
        }
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        return data;
    }

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        return dataStream;
    }
}
