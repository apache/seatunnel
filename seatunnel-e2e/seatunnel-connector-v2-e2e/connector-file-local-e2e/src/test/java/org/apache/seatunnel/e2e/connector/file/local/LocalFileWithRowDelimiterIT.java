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
        disabledReason = "The apache-compress version is not compatible with apache-poi")
public class LocalFileWithRowDelimiterIT extends TestSuiteBase {

    /** Copy data files to container */
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e.txt",
                        "/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                        container);
                container.execInContainer("mkdir", "-p", "/tmp/fake_empty");
            };

    @TestTemplate
    public void testLocalFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);

        // test write local text file
        helper.execute("/text/local_file_text_to_assert_with_row_delimiter.conf");
    }
}
