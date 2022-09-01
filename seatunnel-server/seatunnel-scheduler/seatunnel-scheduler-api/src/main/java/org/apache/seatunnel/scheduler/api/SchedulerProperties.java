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

package org.apache.seatunnel.scheduler.api;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Objects;

@Data
@Configuration
@ConfigurationProperties(prefix = "seatunnel.scheduler")
public class SchedulerProperties {

    private SchedulerTypeEnum type;
    private DolphinScheduler dolphinscheduler = new DolphinScheduler();

    @Data
    public static class DolphinScheduler {
        private String serviceUrl;
        private String token;
        private String projectCode;
        private String tenantCode;
        private HttpClient httpClient;

        public String getTenantCode() {
            return Objects.isNull(tenantCode) || "".equals(tenantCode.trim()) ? "default" : tenantCode;
        }
    }

    @Data
    public static class HttpClient {
        private int connectTimeout;

    }

}
