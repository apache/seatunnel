package org.apache.seatunnel.connectors.seatunnel.druid.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * guanbo
 */
@Data
@AllArgsConstructor
public class DruidSourceOptions implements Serializable {
    private String URL;
    private String datasource ;
    private String startTimestamp ;
    private String endTimestamp ;
    private List<String> columns ;

    private String partitionColumn;
    private Long partitionUpperBound;
    private Long partitionLowerBound;
    private Integer parallelism;

    public DruidSourceOptions(Config pluginConfig) {
        this.URL = pluginConfig.getString(DruidSourceConfig.URL);
        this.datasource = pluginConfig.getString(DruidSourceConfig.DATASOURCE);
        this.columns = pluginConfig.getStringList(DruidSourceConfig.COLUMNS);
        this.startTimestamp = pluginConfig.hasPath(DruidSourceConfig.START_TIMESTAMP) ? pluginConfig.getString(DruidSourceConfig.START_TIMESTAMP) : null;
        this.endTimestamp = pluginConfig.hasPath(DruidSourceConfig.END_TIMESTAMP) ? pluginConfig.getString(DruidSourceConfig.END_TIMESTAMP) : null;
    }
}
