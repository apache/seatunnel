package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;

public class RedshiftJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return "Redshift";
    }
}
