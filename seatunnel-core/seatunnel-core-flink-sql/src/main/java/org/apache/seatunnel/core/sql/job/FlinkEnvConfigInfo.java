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

import org.apache.seatunnel.common.constants.JobMode;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import java.util.List;

public class FlinkEnvConfigInfo {

    private final Config envConfig;

    public Config getEnvConfig() {
        return envConfig;
    }

    public FlinkEnvConfigInfo(List<String> flinkEnvList) {
        this.envConfig = this.buildEnvConfig(flinkEnvList);
    }

    private Config buildEnvConfig(List<String> flinkEnvList) {
        String line = FlinkSqlConstant.SYSTEM_LINE_SEPARATOR;
        String startStr = "env { ".concat(line);
        String endStr = "}";
        StringBuilder sb = new StringBuilder(startStr);
        flinkEnvList.forEach(item -> sb.append(item.concat(line)));
        sb.append(endStr);
        String fullContent = sb.toString();
        Config config = load(fullContent);
        return config.getConfig("env");
    }

    private Config load(String content) {
        Config config = ConfigFactory
                .parseString(content)
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));
        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        return config;
    }

    public JobMode selectJobMode() {
        JobMode jobMode;
        if (envConfig.hasPath("job.mode")) {
            jobMode = envConfig.getEnum(JobMode.class, "job.mode");
        } else {
            jobMode = JobMode.STREAMING;
        }
        return jobMode;
    }
}
