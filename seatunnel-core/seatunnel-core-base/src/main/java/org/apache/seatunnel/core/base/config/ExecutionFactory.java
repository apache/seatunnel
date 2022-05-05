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

package org.apache.seatunnel.core.base.config;

import org.apache.seatunnel.apis.base.api.BaseSink;
import org.apache.seatunnel.apis.base.api.BaseSource;
import org.apache.seatunnel.apis.base.api.BaseTransform;
import org.apache.seatunnel.apis.base.env.Execution;
import org.apache.seatunnel.apis.base.env.RuntimeEnv;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchExecution;
import org.apache.seatunnel.flink.stream.FlinkStreamExecution;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.spark.batch.SparkBatchExecution;
import org.apache.seatunnel.spark.stream.SparkStreamingExecution;
import org.apache.seatunnel.spark.structuredstream.StructuredStreamingExecution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to create {@link Execution}.
 *
 * @param <ENVIRONMENT> environment type
 */
public class ExecutionFactory<ENVIRONMENT extends RuntimeEnv> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionFactory.class);

    public ExecutionContext<ENVIRONMENT> executionContext;

    public ExecutionFactory(ExecutionContext<ENVIRONMENT> executionContext) {
        this.executionContext = executionContext;
    }

    public Execution<BaseSource<ENVIRONMENT>, BaseTransform<ENVIRONMENT>, BaseSink<ENVIRONMENT>, ENVIRONMENT> createExecution() {
        Execution execution = null;
        switch (executionContext.getEngine()) {
            case SPARK:
                SparkEnvironment sparkEnvironment = (SparkEnvironment) executionContext.getEnvironment();
                switch (executionContext.getJobMode()) {
                    case STREAMING:
                        execution = new SparkStreamingExecution(sparkEnvironment);
                        break;
                    case STRUCTURED_STREAMING:
                        execution = new StructuredStreamingExecution(sparkEnvironment);
                        break;
                    default:
                        execution = new SparkBatchExecution(sparkEnvironment);
                }
                break;
            case FLINK:
                FlinkEnvironment flinkEnvironment = (FlinkEnvironment) executionContext.getEnvironment();
                switch (executionContext.getJobMode()) {
                    case STREAMING:
                        execution = new FlinkStreamExecution(flinkEnvironment);
                        break;
                    default:
                        execution = new FlinkBatchExecution(flinkEnvironment);
                }
                break;
            default:
                throw new IllegalArgumentException("No suitable engine");
        }
        LOGGER.info("current execution is [{}]", execution.getClass().getName());
        return (Execution<BaseSource<ENVIRONMENT>, BaseTransform<ENVIRONMENT>, BaseSink<ENVIRONMENT>, ENVIRONMENT>) execution;
    }

}
