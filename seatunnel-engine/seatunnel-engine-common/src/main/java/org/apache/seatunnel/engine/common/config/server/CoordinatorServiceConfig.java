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

package org.apache.seatunnel.engine.common.config.server;

import lombok.Data;

import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

@Data
public class CoordinatorServiceConfig implements Serializable {

    private int coreThreadNum =
            ServerConfigOptions.MasterServerConfigOptions.CORE_THREAD_NUM.defaultValue();

    private int maxThreadNum =
            ServerConfigOptions.MasterServerConfigOptions.MAX_THREAD_NUM.defaultValue();

    public void setCoreThreadNum(int coreThreadNum) {
        checkPositive(
                coreThreadNum,
                ServerConfigOptions.MasterServerConfigOptions.CORE_THREAD_NUM + " must be >= 0");
        this.coreThreadNum = coreThreadNum;
    }

    public void setMaxThreadNum(int maxThreadNum) {
        checkPositive(
                maxThreadNum,
                ServerConfigOptions.MasterServerConfigOptions.MAX_THREAD_NUM + " must be > 0");
        this.maxThreadNum = maxThreadNum;
    }
}
