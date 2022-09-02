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

package org.apache.seatunnel.app.plugin;

import org.apache.seatunnel.scheduler.api.IInstanceService;
import org.apache.seatunnel.scheduler.api.IJobService;
import org.apache.seatunnel.scheduler.api.ISchedulerManager;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;

@Configuration
public class SchedulerPluginHelper {

    private final ISchedulerManager schedulerManager;

    public SchedulerPluginHelper(ISchedulerManager schedulerManager) {
        this.schedulerManager = schedulerManager;
    }

    @Bean
    public IJobService getJobService() {
        return schedulerManager.getJobService();
    }

    @Bean
    public IInstanceService getInstanceService() {
        return schedulerManager.getInstanceService();
    }

    @PreDestroy
    public void close() throws Exception {
        schedulerManager.close();
    }
}
