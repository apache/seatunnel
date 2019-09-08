package io.github.interestinglab.waterdrop.flink.batch;

import com.typesafe.config.waterdrop.Config;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.util.TableUtil;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author mr_xiong
 * @date 2019-08-24 16:26
 * @description
 */
public class FlinkBatchExecution implements Execution<FlinkBatchSource, FlinkBatchTransform, FlinkBatchSink> {

    private Config config;

    private FlinkEnvironment flinkEnvironment;

    private String jobName;

    public FlinkBatchExecution(FlinkEnvironment flinkEnvironment) {
        this.flinkEnvironment = flinkEnvironment;
    }

    @Override
    public void start(List<FlinkBatchSource> sources, List<FlinkBatchTransform> transforms, List<FlinkBatchSink> sinks) {
        List<DataSet> data = new ArrayList<>();

        for (FlinkBatchSource source : sources) {
            DataSet dataSet = source.getData(flinkEnvironment);
            data.add(dataSet);
            registerResultTable(source,dataSet);
        }

        DataSet input = data.get(0);

        for (FlinkBatchTransform transform : transforms) {
            DataSet dataSet = fromSourceTable(transform);
            if (Objects.nonNull(dataSet)){
                input = dataSet;
            }
            input = transform.processBatch(flinkEnvironment, input);
            registerResultTable(transform,input);
        }

        for (FlinkBatchSink sink : sinks) {
            DataSet dataSet = fromSourceTable(sink);
            if (Objects.nonNull(dataSet)){
                input = dataSet;
            }
            sink.outputBatch(flinkEnvironment, input);
        }
        try {
            if (StringUtils.isBlank(jobName)) {
                flinkEnvironment.getBatchEnvironment().execute();
            } else {
                flinkEnvironment.getBatchEnvironment().execute(jobName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void registerResultTable(Plugin plugin, DataSet dataSet) {
        Config config = plugin.getConfig();
        if (config.hasPath(RESULT_TABLE_NAME)) {
            String name = config.getString(RESULT_TABLE_NAME);
            BatchTableEnvironment tableEnvironment = flinkEnvironment.getBatchTableEnvironment();
            if (!TableUtil.tableExists(tableEnvironment,name)) {
                if (config.hasPath("field_name")){
                    String fieldName = config.getString("field_name");
                    tableEnvironment.registerDataSet(name, dataSet,fieldName);
                }else {
                    tableEnvironment.registerDataSet(name, dataSet);
                }
            }
        }
    }


    private DataSet fromSourceTable(Plugin plugin) {
        Config config = plugin.getConfig();
        if (config.hasPath(SOURCE_TABLE_NAME)) {
            BatchTableEnvironment tableEnvironment = flinkEnvironment.getBatchTableEnvironment();
            Table table = tableEnvironment.scan(config.getString(SOURCE_TABLE_NAME));
            return TableUtil.tableToDataSet(tableEnvironment, table);
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
    public void prepare() {
        if (config.hasPath("job.name")) {
            jobName = config.getString("job.name");
        }
    }
}
