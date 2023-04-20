package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import java.util.Optional;
import com.google.auto.service.AutoService;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleDialect;

@AutoService(JdbcDialectFactory.class)
public class OceanBaseOracleDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url, Optional<String> driverTye) {
        return url.startsWith("jdbc:oceanbase:")
                && driverTye.isPresent()
                && driverTye.get().equalsIgnoreCase("oracle");
    }

    @Override
    public JdbcDialect create() {
        return new OracleDialect();
    }
}