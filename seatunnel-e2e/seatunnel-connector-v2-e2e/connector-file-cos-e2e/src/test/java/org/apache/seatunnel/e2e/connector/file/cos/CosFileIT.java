package org.apache.seatunnel.e2e.connector.file.cos;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

@Disabled
public class CosFileIT extends TestSuiteBase {

    @TestTemplate
    public void testCosFileWrite(TestContainer container) throws IOException, InterruptedException {
        // test write cos excel file
        Container.ExecResult excelWriteResult =
                container.executeJob("/excel/fake_to_cos_excel.conf");
        Assertions.assertEquals(0, excelWriteResult.getExitCode(), excelWriteResult.getStderr());

        // test write cos text file
        Container.ExecResult textWriteResult =
                container.executeJob("/text/fake_to_cos_file_text.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());

        // test write cos json file
        Container.ExecResult jsonWriteResult =
                container.executeJob("/json/fake_to_cos_file_json.conf");
        Assertions.assertEquals(0, jsonWriteResult.getExitCode());

        // test write cos orc file
        Container.ExecResult orcWriteResult =
                container.executeJob("/orc/fake_to_cos_file_orc.conf");
        Assertions.assertEquals(0, orcWriteResult.getExitCode());

        // test write cos parquet file
        Container.ExecResult parquetWriteResult =
                container.executeJob("/parquet/fake_to_cos_file_parquet.conf");
        Assertions.assertEquals(0, parquetWriteResult.getExitCode());
    }
}
