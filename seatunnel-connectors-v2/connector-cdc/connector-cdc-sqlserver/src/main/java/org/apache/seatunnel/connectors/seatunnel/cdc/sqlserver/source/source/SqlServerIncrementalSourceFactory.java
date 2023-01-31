package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;

public class SqlServerIncrementalSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return SqlServerIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
            .required(
                JdbcSourceOptions.HOSTNAME,
                JdbcSourceOptions.USERNAME,
                JdbcSourceOptions.PASSWORD,
                JdbcSourceOptions.DATABASE_NAME,
                JdbcSourceOptions.TABLE_NAME)
            .optional(
                JdbcSourceOptions.PORT,
                JdbcSourceOptions.SERVER_TIME_ZONE,
                JdbcSourceOptions.CONNECT_TIMEOUT_MS,
                JdbcSourceOptions.CONNECT_MAX_RETRIES,
                JdbcSourceOptions.CONNECTION_POOL_SIZE)
            .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SqlServerIncrementalSource.class;
    }
}
