/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/8/20 16:26
 */
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;


public class OracleDialect implements JdbcDialect {
	@Override
	public String dialectName() {
		return "Oracle";
	}

	@Override
	public JdbcRowConverter getRowConverter() {
		return new OracleJdbcRowConverter();
	}

	@Override
	public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
		return new OracleTypeMapper();
	}
}
