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

package org.apache.seatunnel.e2e.common.container;

import static org.apache.seatunnel.e2e.common.container.EngineType.FLINK;
import static org.apache.seatunnel.e2e.common.container.EngineType.SPARK;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum TestContainerId {
    FLINK_1_13(FLINK, "1.13.6"),
    SPARK_2_4(SPARK, "2.4.6"),
    SEATUNNEL(EngineType.SEATUNNEL, "2.2.0");

    private final EngineType engineType;
    private final String version;

    @Override
    public String toString() {
        return engineType.toString() + ":" + version;
    }
}
