package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.debezium.jdbc.JdbcConnection;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OracleConnectionUtilsTest {

    private static final String SHOW_CON_NAME =
            "SELECT SYS_CONTEXT('USERENV', 'CON_NAME') CON_NAME FROM DUAL";

    @Mock private JdbcConnection jdbcConnection;

    @Test
    public void testGetCurrentContainerNameSuccess() throws SQLException {
        // Prepare test data
        String expectedContainerName = "CDB$ROOT";

        // Mock database query result
        when(jdbcConnection.queryAndMap(eq(SHOW_CON_NAME), any()))
                .thenReturn(expectedContainerName);

        // Execute test
        String actualContainerName = OracleConnectionUtils.getCurrentContainerName(jdbcConnection);

        // Verify result
        assertEquals(expectedContainerName, actualContainerName);
    }
}
