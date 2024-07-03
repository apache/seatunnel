package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class DorisNoSchemaIT extends DorisIT {

    @TestTemplate
    public void testDoris(TestContainer container) throws IOException, InterruptedException {
        initializeJdbcTable();
        batchInsertUniqueTableData();
        Container.ExecResult execResult1 = container.executeJob("/doris_source_no_schema.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());
        checkSinkData();
    }
}
