package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;

import com.google.auto.service.AutoService;

@AutoService(JdbcDialectFactory.class)
public class RedshiftDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:redshift:");
    }

    @Override
    public JdbcDialect create() {
        return new RedshiftDialect();
    }
}
