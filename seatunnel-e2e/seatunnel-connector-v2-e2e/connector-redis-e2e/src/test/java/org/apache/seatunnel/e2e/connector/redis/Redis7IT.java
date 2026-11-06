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
package org.apache.seatunnel.e2e.connector.redis;

import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisContainerInfo;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@ResourceLock("redis-standalone-e2e")
public class Redis7IT extends RedisTestCaseTemplateIT {

    @TestTemplate
    public void testNamedUserSourceAndSink(TestContainer container)
            throws IOException, InterruptedException {
        jedis.aclSetUser(
                "seatunnel_reader",
                "reset",
                "on",
                ">reader-password",
                "~acl:source:*",
                "+select",
                "+info",
                "+scan",
                "+type",
                "+get",
                "+mget");
        jedis.aclSetUser(
                "seatunnel_writer",
                "reset",
                "on",
                ">writer-password",
                "~acl:result",
                "+select",
                "+info",
                "+lpush");
        List<String> aclBefore = jedis.aclList();
        jedis.set("acl:source:1", "{\"value\":\"named-user\"}");
        try {
            Container.ExecResult result = container.executeJob("/redis-named-user.conf");
            Assertions.assertEquals(0, result.getExitCode());
            Assertions.assertEquals(
                    Collections.singletonList("named-user"), jedis.lrange("acl:result", 0, -1));
            Assertions.assertEquals(aclBefore, jedis.aclList());
        } finally {
            jedis.del("acl:source:1", "acl:result");
            jedis.aclDelUser("seatunnel_reader", "seatunnel_writer");
        }
    }

    @Override
    public RedisContainerInfo getRedisContainerInfo() {
        return new RedisContainerInfo("redis-e2e", 6379, "SeaTunnel", "redis:7");
    }
}
