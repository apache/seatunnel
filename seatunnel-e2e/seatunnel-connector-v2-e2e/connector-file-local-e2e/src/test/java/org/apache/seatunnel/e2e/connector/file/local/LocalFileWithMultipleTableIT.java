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

package org.apache.seatunnel.e2e.connector.file.local;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.TestTemplate;

import java.io.IOException;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {},
        disabledReason = "")
public class LocalFileWithMultipleTableIT extends TestSuiteBase {

    /** Copy data files to container */
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                ContainerUtil.copyFileIntoContainers(
                        "/excel/e2e.xlsx",
                        "/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/json/e2e.json",
                        "/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/orc/e2e.orc",
                        "/seatunnel/read/orc/name=tyrantlucifer/hobby=coding/e2e.orc",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/parquet/e2e.parquet",
                        "/seatunnel/read/parquet/name=tyrantlucifer/hobby=coding/e2e.parquet",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e.txt",
                        "/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/binary/cat.png",
                        "/seatunnel/read/binary/name=tyrantlucifer/hobby=coding/cat.png",
                        container);

                container.execInContainer("mkdir", "-p", "/tmp/fake_empty");
            };

    @TestTemplate
    public void testFakeToLocalFileInMultipleTableMode_text(TestContainer testContainer)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(testContainer);
        helper.execute("/text/fake_to_local_file_with_multiple_table.conf");
    }

    @TestTemplate
    public void testLocalFileReadAndWriteInMultipleTableMode_excel(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/excel/local_excel_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testLocalFileReadAndWriteInMultipleTableMode_json(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/json/local_file_json_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testLocalFileReadAndWriteInMultipleTableMode_orc(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/orc/local_file_orc_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testLocalFileReadAndWriteInMultipleTableMode_parquet(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/parquet/local_file_parquet_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testLocalFileReadAndWriteInMultipleTableMode_text(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/text/local_file_text_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testLocalFileReadAndWriteInMultipleTableMode_binary(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/binary/local_file_binary_to_local_file_binary_with_multipletable.conf");
    }
}
