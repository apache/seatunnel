package org.apache.seatunnel.e2e.transform;


import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TestPythonTransformIT extends TestSuiteBase{

    @TestTemplate
    public void TestPythonTransform(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/field_mapper_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Container.ExecResult execResult1 =
                container.executeJob("/field_mapper_transform_without_result_table.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());
    }
}
