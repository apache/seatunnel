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
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;

import org.jdom.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;

import com.aliyun.oss.OSS;

import java.io.IOException;

@Disabled("Disabled because it needs user's personal oss account to run this test")
public class OssFileWithMultipleTableIT extends TestSuiteBase {

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.of(OSS.class).copyTo(container, "/tmp/seatunnel/plugins/oss/lib");
                DependencyJar.of(Document.class)
                        .copyTo(container, "/tmp/seatunnel/plugins/oss/lib");
                DependencyJar.of(AliyunOSSFileSystem.class)
                        .copyTo(container, "/tmp/seatunnel/plugins/oss/lib");
                DependencyJar.of(OSS.class).copyTo(container, "/tmp/seatunnel/lib");
                DependencyJar.of(Document.class).copyTo(container, "/tmp/seatunnel/lib");
                DependencyJar.of(AliyunOSSFileSystem.class).copyTo(container, "/tmp/seatunnel/lib");
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
