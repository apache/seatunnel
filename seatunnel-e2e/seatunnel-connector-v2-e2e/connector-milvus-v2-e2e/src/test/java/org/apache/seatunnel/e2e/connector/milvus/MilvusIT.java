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

package org.apache.seatunnel.e2e.connector.milvus;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.QueryResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.dml.QueryParam;

import java.io.IOException;

@Disabled
public class MilvusIT extends TestSuiteBase implements TestResource {

    private MilvusServiceClient milvusClient;

    private final String host = "172.30.10.80";

    private final Integer port = 19530;

    private final String username = "root";

    private final String password = "Milvus";

    private final String collectionName = "title_db";

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        ConnectParam connectParam =
                ConnectParam.newBuilder()
                        .withHost(host)
                        .withPort(port)
                        .withAuthorization(username, password)
                        .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (milvusClient != null) {
            milvusClient.close();
        }
    }

    @TestTemplate
    public void testMilvus(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake_to_milvus.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        R<QueryResults> query =
                milvusClient.query(
                        QueryParam.newBuilder().withCollectionName(collectionName).build());

        Assertions.assertEquals(5, query.getData().getFieldsDataCount());
    }
}
