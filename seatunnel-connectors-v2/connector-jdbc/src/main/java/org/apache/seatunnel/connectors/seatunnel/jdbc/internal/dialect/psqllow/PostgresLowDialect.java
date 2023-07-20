package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psqllow;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;

import java.util.Optional;

public class PostgresLowDialect extends PostgresDialect {
    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }
}
