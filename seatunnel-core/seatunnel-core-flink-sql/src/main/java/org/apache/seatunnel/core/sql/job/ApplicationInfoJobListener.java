/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.sql.job;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

import java.lang.reflect.Field;

public class ApplicationInfoJobListener implements JobListener {

    private StreamExecutionEnvironment streamExecutionEnvironment;

    private ExecutionEnvironment batchEnvironment;

    public ApplicationInfoJobListener(StreamExecutionEnvironment streamExecutionEnvironment, ExecutionEnvironment batchEnvironment) {
        this.streamExecutionEnvironment = streamExecutionEnvironment;
        this.batchEnvironment = batchEnvironment;
    }

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        ConfigOption<String> applicationId = ConfigOptions.key("yarn.application.id").stringType().noDefaultValue();
        try {
            Configuration configuration;
            if (streamExecutionEnvironment != null) {
                Field configurationField = StreamExecutionEnvironment.class.getDeclaredField("configuration");
                if (!configurationField.isAccessible()) {
                    configurationField.setAccessible(true);
                }
                configuration = (Configuration) configurationField.get(streamExecutionEnvironment);
            } else {
                configuration = batchEnvironment.getConfiguration();
            }
            String appId = configuration.get(applicationId);
            LogPrint.logPrint("print flink applicationId", appId);
        } catch (Exception e) {
            LogPrint.logPrint("print JobListener exception log", e.getMessage());
        }
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {

    }
}
