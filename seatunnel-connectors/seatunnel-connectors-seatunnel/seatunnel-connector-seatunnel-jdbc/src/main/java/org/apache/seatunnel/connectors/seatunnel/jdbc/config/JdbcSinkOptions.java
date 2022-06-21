package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConfig.buildJdbcConnectionOptions;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * @Author: Liuli
 * @Date: 2022/6/15 21:11
 */
@Data
@AllArgsConstructor
public class JdbcSinkOptions implements Serializable {
    private JdbcConnectionOptions jdbcConnectionOptions;
    private boolean isExactlyOnce;

    public JdbcSinkOptions(Config config) {
        this.jdbcConnectionOptions = buildJdbcConnectionOptions(config);
        if (config.hasPath(JdbcConfig.IS_EXACTLY_ONCE) && config.getBoolean(JdbcConfig.IS_EXACTLY_ONCE)) {
            this.isExactlyOnce = true;
        }
    }
}
