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

package org.apache.seatunnel.engine.server.rest;

public class RestConstant {

    public static final String RUNNING_JOBS_URL = "/hazelcast/rest/maps/running-jobs";
    public static final String RUNNING_JOB_URL = "/hazelcast/rest/maps/running-job";
    public static final String SUBMIT_JOB_URL = "/hazelcast/rest/maps/submit-job";

    public static final String SYSTEM_MONITORING_INFORMATION =
            "/hazelcast/rest/maps/system-monitoring-information";
}
