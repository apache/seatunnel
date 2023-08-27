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

package org.apache.seatunnel.e2e.connector.amazonsqs;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import software.amazon.awssdk.services.sqs.SqsClient;
import org.testcontainers.containers.localstack.LocalStackContainer;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AmazonsqsIT extends TestSuiteBase implements TestResource {
    private static final String LOCALSTACK_DOCKER_IMAGE = "localstack/localstack:0.11.3";
    private static final String AMAZONSQS_JOB_CONFIG = "/amazonsqsIT_source_to_sink.conf";
    private static final String SINK_TABLE = "sink_table";
    private static final String SOURCE_TABLE = "source_table";
    private static final String PARTITION_KEY = "id";

    protected SqsClient sqsClient;
    
    private LocalStackContainer localstack;

    @Override
    public void startUp() throws Exception {
        // start a localstack docker container
        localstack = new LocalStackContainer(LOCALSTACK_DOCKER_IMAGE)
                .withServices(LocalStackContainer.Service.SQS);

        // create a sqs client
        sqsClient = SqsClient.builder()
                .endpointOverride(localstack.getEndpoint())
                .build();
    }

    @Override
    public void tearDown() throws Exception {
        if (localstack != null) {
            localstack.close();
        }
    }

    @TestTemplate
    public void testAmazonSqs(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob(AMAZONSQS_JOB_CONFIG);
    }
}
