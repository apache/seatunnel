package io.github.interestinglab.waterdrop.flink.stream;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.util.TableUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FlinkStreamExecution implements Execution<FlinkStreamSource, FlinkStreamTransform, FlinkStreamSink> {

    private Config config;

    private FlinkEnvironment flinkEnvironment;


    public FlinkStreamExecution(FlinkEnvironment streamEnvironment) {
        this.flinkEnvironment = streamEnvironment;
    }

    @Override
    public void start(List<FlinkStreamSource> sources, List<FlinkStreamTransform> transforms, List<FlinkStreamSink> sinks) {
        List<DataStream> data = new ArrayList<>();

        for (FlinkStreamSource source : sources) {
            DataStream dataStream = source.getData(flinkEnvironment);
            data.add(dataStream);
            registerResultTable(source, dataStream);
        }

        DataStream input = data.get(0);

        for (FlinkStreamTransform transform : transforms) {
            DataStream stream = fromSourceTable(transform);
            if (Objects.isNull(stream)) {
                stream = input;
            }
            input = transform.processStream(flinkEnvironment, stream);
            registerResultTable(transform, input);
            transform.registerFunction(flinkEnvironment);
        }

        for (FlinkStreamSink sink : sinks) {
            DataStream stream = fromSourceTable(sink);
            if (Objects.isNull(stream)) {
                stream = input;
            }
            sink.outputStream(flinkEnvironment, stream);
        }
        try {
            flinkEnvironment.getStreamExecutionEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void registerResultTable(Plugin plugin, DataStream dataStream) {
        Config config = plugin.getConfig();
        if (config.hasPath(RESULT_TABLE_NAME)) {
            String name = config.getString(RESULT_TABLE_NAME);
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            if (!TableUtil.tableExists(tableEnvironment, name)) {
                if (config.hasPath("field_name")) {
                    String fieldName = config.getString("field_name");
                    tableEnvironment.registerDataStream(name, dataStream, fieldName);
                } else {
                    tableEnvironment.registerDataStream(name, dataStream);
                }
            }
        }
    }

    private DataStream fromSourceTable(Plugin plugin) {
        Config config = plugin.getConfig();
        if (config.hasPath(SOURCE_TABLE_NAME)) {
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            Table table = tableEnvironment.scan(config.getString(SOURCE_TABLE_NAME));
            return TableUtil.tableToDataStream(tableEnvironment, table, true);
        }
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
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(Void prepareEnv) {
    }
}
