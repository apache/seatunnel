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

import lombok.AllArgsConstructor;
import lombok.Getter;

import static org.apache.seatunnel.e2e.common.container.EngineType.FLINK;
import static org.apache.seatunnel.e2e.common.container.EngineType.SPARK;

@AllArgsConstructor
@Getter
public enum TestContainerId {
    FLINK_1_13(FLINK, "1.13.6"),
    FLINK_1_14(FLINK, "1.14.6"),
    FLINK_1_15(FLINK, "1.15.3"),
    FLINK_1_16(FLINK, "1.16.0"),
    SPARK_2_4(SPARK, "2.4.6"),
    SPARK_3_3(SPARK, "3.3.0"),
    SEATUNNEL(EngineType.SEATUNNEL, "2.3.1");

    private final EngineType engineType;
    private final String version;

    @Override
    public String toString() {
        return engineType.toString() + ":" + version;
    }
}
