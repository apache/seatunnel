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

package org.apache.seatunnel.e2e.connector.file.s3;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;

import java.io.IOException;

@Disabled("have no s3 environment to run this test")
public class S3FileWithMultipleTableIT extends TestSuiteBase {

    /**
     * Put S3A on the container's classpath the same way the distribution does, with the shaded
     * {@code seatunnel-hadoop-aws} jar.
     *
     * <p>This used to {@code curl} {@code hadoop-aws} 3.1.4 and the AWS SDK v1 bundle from Maven
     * Central into both the plugin dir and {@code lib/}. Both are incompatible with the {@code
     * hadoop-common} the uber jar now ships. The shaded jar carries {@code hadoop-aws} together
     * with its own SDK, which is what {@code lib/seatunnel-hadoop-aws.jar} is in a real
     * distribution, and it is staged into the test classpath by the maven-dependency-plugin rather
     * than downloaded.
     */
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar shadedHadoopAws = DependencyJar.staged("seatunnel-hadoop-aws.jar");
                shadedHadoopAws.copyTo(container, "/tmp/seatunnel/plugins/s3/lib");
                shadedHadoopAws.copyTo(container, "/tmp/seatunnel/lib");
            };

    /** Copy data files to s3 */
    @TestTemplate
    public void addTestFiles(TestContainer container) throws IOException, InterruptedException {
        // Copy test files to s3
        S3Utils.uploadTestFiles(
                "/json/e2e.json",
                "test/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                true);
        S3Utils.uploadTestFiles(
                "/text/e2e.txt",
                "test/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                true);
        S3Utils.uploadTestFiles(
                "/excel/e2e.xlsx",
                "test/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                true);
        S3Utils.uploadTestFiles(
                "/orc/e2e.orc",
                "test/seatunnel/read/orc/name=tyrantlucifer/hobby=coding/e2e.orc",
                true);
        S3Utils.uploadTestFiles(
                "/parquet/e2e.parquet",
                "test/seatunnel/read/parquet/name=tyrantlucifer/hobby=coding/e2e.parquet",
                true);
        S3Utils.createDir("tmp/fake_empty");
    }

    @TestTemplate
    public void testFakeToS3FileInMultipleTableMode_text(TestContainer testContainer)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(testContainer);
        helper.execute("/text/fake_to_s3_file_with_multiple_table.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_excel(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/excel/s3_excel_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_json(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/json/s3_file_json_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_orc(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/orc/s3_file_orc_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_parquet(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/parquet/s3_file_parquet_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_text(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/text/s3_file_text_to_assert_with_multipletable.conf");
    }
}
