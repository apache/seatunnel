/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.apis;

import io.github.interestinglab.waterdrop.config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Define an action to do whatever you want on execution started or finished,
 * the sequence will be as follows:
 *     program started ..
 *     BaseAction::onExecutionStarted()
 *       for loop for data:
 *         Source --> Transform --> Sink
 *     BaseAction::onExecutionFinished()
 *     program finished ..
 *
 * Attention:
 *   1. For now, this only apply for batch processing, not for streaming or structured streaming processing
 * */
public interface BaseAction extends Plugin {

  void onExecutionStarted(SparkSession sparkSession, SparkConf sparkConf, Config config);

  void onExecutionFinished(SparkConf sparkConf, Config config);
}
