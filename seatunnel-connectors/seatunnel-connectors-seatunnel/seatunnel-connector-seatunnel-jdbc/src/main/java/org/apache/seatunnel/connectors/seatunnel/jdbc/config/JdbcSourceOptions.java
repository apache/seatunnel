package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConfig.buildJdbcConnectionOptions;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Optional;

/**
 * @Author: Liuli
 * @Date: 2022/6/15 21:11
 */
@Data
@AllArgsConstructor
public class JdbcSourceOptions implements Serializable {
    private JdbcConnectionOptions jdbcConnectionOptions;
    private String partitionColumn;
    private Long partitionUpperBound;
    private Long partitionLowerBound;

    private Integer parallelism;

    public JdbcSourceOptions(Config config) {
        this.jdbcConnectionOptions = buildJdbcConnectionOptions(config);
        if (config.hasPath(JdbcConfig.PARTITION_COLUMN)) {
            this.partitionColumn = config.getString(JdbcConfig.PARTITION_COLUMN);
        }
        if (config.hasPath(JdbcConfig.PARTITION_UPPER_BOUND)) {
            this.partitionUpperBound = config.getLong(JdbcConfig.PARTITION_UPPER_BOUND);
        }
        if (config.hasPath(JdbcConfig.PARTITION_LOWER_BOUND)) {
            this.partitionLowerBound = config.getLong(JdbcConfig.PARTITION_LOWER_BOUND);
        }
        if (config.hasPath(JdbcConfig.PARALLELISM)) {
            this.parallelism = config.getInt(JdbcConfig.PARALLELISM);
        }
    }

    public JdbcConnectionOptions getJdbcConnectionOptions() {
        return jdbcConnectionOptions;
    }

    public Optional<String> getPartitionColumn() {
        return Optional.ofNullable(partitionColumn);
    }

    public Optional<Long> getPartitionUpperBound() {
        return Optional.ofNullable(partitionUpperBound);
    }

    public Optional<Long> getPartitionLowerBound() {
        return Optional.ofNullable(partitionLowerBound);
    }

    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(parallelism);
    }
}
