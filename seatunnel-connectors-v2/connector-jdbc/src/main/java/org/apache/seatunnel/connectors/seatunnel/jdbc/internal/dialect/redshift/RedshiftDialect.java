package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.util.Optional;

public class RedshiftDialect  implements JdbcDialect {
    @Override
    public String dialectName() {
        return "Redshift";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new RedshiftJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new RedshiftTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }
}
