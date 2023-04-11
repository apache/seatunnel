package org.apache.seatunnel.core.starter.flink.transforms;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.core.starter.flink.execution.FlinkRuntimeEnvironment;
import org.apache.seatunnel.core.starter.flink.utils.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.google.auto.service.AutoService;

@AutoService(FlinkTransform.class)
public class Sql extends AbstractFlinkTransform {
    private String sql;

    private static final String SQL = "sql";

    @Override
    public DataStream<Row> processStream(FlinkRuntimeEnvironment env, DataStream<Row> dataStream)
            throws Exception {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = null;
        try {
            table = tableEnvironment.sqlQuery(sql);
        } catch (Exception e) {
            throw new Exception("Flink streaming transform sql execute failed, SQL: " + sql, e);
        }
        return TableUtil.tableToDataStream(tableEnvironment, table, false);
    }

    @Override
    public void setConfig(Config config) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(config, SQL);
        if (!checkResult.isSuccess()) {
            throw new IllegalArgumentException("Failed to check config! " + checkResult.getMsg());
        }
        sql = config.getString(SQL);
    }

    @Override
    public String getPluginName() {
        return "flink-sql";
    }
}
