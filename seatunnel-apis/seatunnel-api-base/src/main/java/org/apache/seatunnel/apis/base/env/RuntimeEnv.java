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

package org.apache.seatunnel.apis.base.env;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.List;

/**
 * engine related runtime environment
 */
public interface RuntimeEnv {

    RuntimeEnv setConfig(Config config);

    Config getConfig();

    CheckResult checkConfig();

    RuntimeEnv prepare();

    RuntimeEnv setJobMode(JobMode mode);

    JobMode getJobMode();

    void registerPlugin(List<URL> pluginPaths);

}
