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

package org.apache.seatunnel.core.starter.flowcontrol;

import org.apache.seatunnel.api.env.EnvCommonOptions;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class FlowControlStrategy {

    int bytesPerSecond;
    int countPreSecond;

    public FlowControlStrategy(int bytesPerSecond, int countPreSecond) {
        if (bytesPerSecond <= 0 || countPreSecond <= 0) {
            throw new IllegalArgumentException(
                    "bytesPerSecond and countPreSecond must be positive");
        }
        this.bytesPerSecond = bytesPerSecond;
        this.countPreSecond = countPreSecond;
    }

    public static FlowControlStrategy of(int bytesPerSecond, int countPreSecond) {
        return new FlowControlStrategy(bytesPerSecond, countPreSecond);
    }

    public static FlowControlStrategy ofBytes(int bytesPerSecond) {
        return new FlowControlStrategy(bytesPerSecond, Integer.MAX_VALUE);
    }

    public static FlowControlStrategy ofCount(int countPreSecond) {
        return new FlowControlStrategy(Integer.MAX_VALUE, countPreSecond);
    }

    // Build the FlowControlStrategy object based on your configured speed limiting parameters
    public static FlowControlStrategy getFlowControlStrategy(Map<String, Object> envOption) {
        FlowControlStrategy strategy;
        if (envOption == null || envOption.isEmpty()) {
            return null;
        }
        if (envOption.containsKey(EnvCommonOptions.READ_LIMIT_BYTES_PER_SECOND.key())
                && envOption.containsKey(EnvCommonOptions.READ_LIMIT_ROW_PER_SECOND.key())) {
            strategy =
                    FlowControlStrategy.of(
                            Integer.parseInt(
                                    envOption
                                            .get(EnvCommonOptions.READ_LIMIT_BYTES_PER_SECOND.key())
                                            .toString()),
                            Integer.parseInt(
                                    envOption
                                            .get(EnvCommonOptions.READ_LIMIT_ROW_PER_SECOND.key())
                                            .toString()));
        } else if (envOption.containsKey(EnvCommonOptions.READ_LIMIT_BYTES_PER_SECOND.key())) {
            strategy =
                    FlowControlStrategy.ofBytes(
                            Integer.parseInt(
                                    envOption
                                            .get(EnvCommonOptions.READ_LIMIT_BYTES_PER_SECOND.key())
                                            .toString()));
        } else if (envOption.containsKey(EnvCommonOptions.READ_LIMIT_ROW_PER_SECOND.key())) {
            strategy =
                    FlowControlStrategy.ofCount(
                            Integer.parseInt(
                                    envOption
                                            .get(EnvCommonOptions.READ_LIMIT_ROW_PER_SECOND.key())
                                            .toString()));
        } else {
            strategy = null;
        }
        return strategy;
    }
}
