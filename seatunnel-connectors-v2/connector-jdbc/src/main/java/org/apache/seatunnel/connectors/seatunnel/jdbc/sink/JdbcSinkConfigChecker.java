package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSinkConfigChecker {

    public static void check(ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.get(JdbcSinkOptions.USE_SQLSERVER_BULK_COPY)) {
            if (readonlyConfig.get(JdbcSinkOptions.AUTO_COMMIT)) {
                log.warn(
                        "When use_sqlserver_bulk_copy is enabled, auto_commit is true and does not take effect.");
            }
            if (readonlyConfig.get(JdbcSinkOptions.IS_EXACTLY_ONCE)) {
                log.warn(
                        "When use_sqlserver_bulk_copy is enabled, is_exactly_once is true and does not take effect.");
            }
            if (readonlyConfig.get(JdbcSinkOptions.ENABLE_UPSERT)) {
                log.warn(
                        "When use_sqlserver_bulk_copy is enabled, enable_upsert is true and does not take effect.");
            }
        }
    }
}
