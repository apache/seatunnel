package org.apache.seatunnel.connectors.seatunnel.jdbc;

public class JdbcPrePostSQLIT extends JdbcOracledbIT {
    private static final String CONFIG_FILE = "/jdbc_sink_pre_post_sql.conf";

    @Override
    JdbcCase getJdbcCase() {
        JdbcCase jdbcCase = super.getJdbcCase();
        jdbcCase.setConfigFile(CONFIG_FILE);
        return jdbcCase;
    }
}
