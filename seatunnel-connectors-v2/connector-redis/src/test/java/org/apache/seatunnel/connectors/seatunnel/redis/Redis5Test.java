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
package org.apache.seatunnel.connectors.seatunnel.redis;

import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisContainerInfo;

import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "There is no docker environment on the windows test system")
public class Redis5Test extends RedisTemplateTest {

    @Override
    public RedisContainerInfo getRedisContainerInfo() {
        return new RedisContainerInfo("redis-e2e", 6379, "SeaTunnel", "redis:5");
    }
}
