package org.apache.seatunnel.connectors.seatunnel.druid.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.joda.time.DateTime;

import java.io.Serializable;

/**
 * guanbo
 */
@Data
@AllArgsConstructor
public class DruidSinkOptions implements Serializable {
    private  String coordinatorURL;
    private  String datasource;
    private  String timestampColumn;
    private  String timestampFormat;
    private  String  timestampMissingValue;
    private  String  columns;
    private  int parallelism;

    private static final String DEFAULT_TIMESTAMP_COLUMN = "timestamp";
    private static final String DEFAULT_TIMESTAMP_FORMAT = "auto";
    private static final DateTime DEFAULT_TIMESTAMP_MISSING_VALUE = null;
    private static final int DEFAULT_PARALLELISM = 1;

    public DruidSinkOptions(Config pluginConfig) {
        this.coordinatorURL = pluginConfig.getString(DruidSinkConfig.COORDINATOR_URL);
        this.datasource = pluginConfig.getString(DruidSinkConfig.DATASOURCE);
        this.columns = pluginConfig.getString(DruidSinkConfig.COLUMNS);
        this.timestampColumn = pluginConfig.hasPath(DruidSinkConfig.TIMESTAMP_COLUMN) ? pluginConfig.getString(DruidSinkConfig.TIMESTAMP_COLUMN) : null;
        this.timestampFormat = pluginConfig.hasPath(DruidSinkConfig.TIMESTAMP_FORMAT) ? pluginConfig.getString(DruidSinkConfig.TIMESTAMP_FORMAT) : null;
        this.timestampMissingValue = pluginConfig.hasPath(DruidSinkConfig.TIMESTAMP_MISSING_VALUE) ? pluginConfig.getString(DruidSinkConfig.TIMESTAMP_MISSING_VALUE) : null;
    }
}
