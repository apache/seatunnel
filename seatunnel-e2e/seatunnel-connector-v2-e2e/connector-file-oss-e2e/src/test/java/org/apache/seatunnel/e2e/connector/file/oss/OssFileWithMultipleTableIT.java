/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.file.oss;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

@Disabled("Disabled because it needs user's personal oss account to run this test")
public class OssFileWithMultipleTableIT extends TestSuiteBase {

    public static final String OSS_SDK_DOWNLOAD =
            "https://repo1.maven.org/maven2/com/aliyun/oss/aliyun-sdk-oss/3.4.1/aliyun-sdk-oss-3.4.1.jar";
    public static final String JDOM_DOWNLOAD =
            "https://repo1.maven.org/maven2/org/jdom/jdom/1.1/jdom-1.1.jar";
    public static final String HADOOP_ALIYUN_DOWNLOAD =
            "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aliyun/3.1.4/hadoop-aliyun-3.1.4.jar";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/oss/lib && cd /tmp/seatunnel/plugins/oss/lib && curl -O "
                                        + OSS_SDK_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/plugins/oss/lib && curl -O " + JDOM_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/plugins/oss/lib && curl -O "
                                        + HADOOP_ALIYUN_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/lib && curl -O " + OSS_SDK_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash", "-c", "cd /tmp/seatunnel/lib && curl -O " + JDOM_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/lib && curl -O " + HADOOP_ALIYUN_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    /** Copy data files to oss */
    @TestTemplate
    public void addTestFiles(TestContainer container) throws IOException, InterruptedException {
        // Copy test files to OSS
        OssUtils ossUtils = new OssUtils();
        try {
            ossUtils.uploadTestFiles(
                    "/json/e2e.json",
                    "test/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                    true);
            ossUtils.uploadTestFiles(
                    "/text/e2e.txt",
                    "test/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                    true);
            ossUtils.uploadTestFiles(
                    "/excel/e2e.xlsx",
                    "test/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                    true);
            ossUtils.uploadTestFiles(
                    "/orc/e2e.orc",
                    "test/seatunnel/read/orc/name=tyrantlucifer/hobby=coding/e2e.orc",
                    true);
            ossUtils.uploadTestFiles(
                    "/parquet/e2e.parquet",
                    "test/seatunnel/read/parquet/name=tyrantlucifer/hobby=coding/e2e.parquet",
                    true);
            ossUtils.createDir("tmp/fake_empty");
        } finally {
            ossUtils.close();
        }
    }

    @TestTemplate
    public void testFakeToOssFileInMultipleTableMode_text(TestContainer testContainer)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(testContainer);
        helper.execute("/text/fake_to_oss_file_with_multiple_table.conf");
    }

    @TestTemplate
    public void testOssFileReadAndWriteInMultipleTableMode_excel(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/excel/oss_excel_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testOssFileReadAndWriteInMultipleTableMode_json(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/json/oss_file_json_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testOssFileReadAndWriteInMultipleTableMode_orc(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/orc/oss_file_orc_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testOssFileReadAndWriteInMultipleTableMode_parquet(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/parquet/oss_file_parquet_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testOssFileReadAndWriteInMultipleTableMode_text(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/text/oss_file_text_to_assert_with_multipletable.conf");
    }
}
