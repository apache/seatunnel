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

package org.apache.seatunnel.scheduler.dolphinscheduler;

import org.apache.seatunnel.scheduler.api.IInstanceService;
import org.apache.seatunnel.scheduler.api.IJobService;
import org.apache.seatunnel.scheduler.api.ISchedulerManager;
import org.apache.seatunnel.scheduler.api.SchedulerProperties;
import org.apache.seatunnel.scheduler.dolphinscheduler.impl.DolphinSchedulerServiceImpl;
import org.apache.seatunnel.scheduler.dolphinscheduler.impl.InstanceServiceImpl;
import org.apache.seatunnel.scheduler.dolphinscheduler.impl.JobServiceImpl;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "seatunnel.scheduler", name = "type", havingValue = "dolphinscheduler")
public class DolphinSchedulerManager implements ISchedulerManager {

    private final IDolphinSchedulerService dolphinSchedulerService;
    private final IInstanceService instanceService;

    public DolphinSchedulerManager(SchedulerProperties properties) {
        dolphinSchedulerService = new DolphinSchedulerServiceImpl(properties.getDolphinscheduler());
        instanceService = new InstanceServiceImpl(dolphinSchedulerService);
    }

    @Override
    public IJobService getJobService() {
        return new JobServiceImpl(dolphinSchedulerService, instanceService);
    }

    @Override
    public IInstanceService getInstanceService() {
        return instanceService;
    }

    @Override
    public void close() throws Exception {

    }
}
