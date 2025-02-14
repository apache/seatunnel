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

package org.apache.seatunnel.api.options;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class EnvOptionRule implements Factory {

    @Override
    public String factoryIdentifier() {
        return "EnvOptionRule";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(EnvCommonOptions.JOB_MODE)
                .optional(
                        EnvCommonOptions.JOB_NAME,
                        EnvCommonOptions.PARALLELISM,
                        EnvCommonOptions.JOB_RETRY_TIMES,
                        EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS,
                        EnvCommonOptions.JARS,
                        EnvCommonOptions.CHECKPOINT_INTERVAL,
                        EnvCommonOptions.CHECKPOINT_TIMEOUT,
                        EnvCommonOptions.READ_LIMIT_ROW_PER_SECOND,
                        EnvCommonOptions.READ_LIMIT_BYTES_PER_SECOND,
                        EnvCommonOptions.SAVEMODE_EXECUTE_LOCATION,
                        EnvCommonOptions.CUSTOM_PARAMETERS,
                        EnvCommonOptions.NODE_TAG_FILTER)
                .build();
    }
}
