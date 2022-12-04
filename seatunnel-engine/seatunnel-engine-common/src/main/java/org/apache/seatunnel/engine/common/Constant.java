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

package org.apache.seatunnel.engine.common;

public class Constant {
    public static final String SEATUNNEL_SERVICE_NAME = "st:impl:seaTunnelServer";

    public static final String SEATUNNEL_ID_GENERATOR_NAME = "SeaTunnelIdGenerator";

    public static final String DEFAULT_SEATUNNEL_CLUSTER_NAME = "seatunnel";

    /**
     * The default port number for the cluster auto-discovery mechanism's
     * multicast communication.
     */
    public static final int DEFAULT_SEATUNNEL_MULTICAST_PORT = 53326;

    public static final String SYSPROP_SEATUNNEL_CONFIG = "seatunnel.config";

    public static final String HAZELCAST_SEATUNNEL_CONF_FILE_PREFIX = "seatunnel";

    public static final String HAZELCAST_SEATUNNEL_DEFAULT_YAML = "seatunnel.yaml";

    public static final int OPERATION_RETRY_TIME = 10;

    public static final int OPERATION_RETRY_SLEEP = 2000;

    public static final String IMAP_RUNNING_JOB_INFO = "runningJobInfo";

    public static final String IMAP_RUNNING_JOB_STATE = "runningJobState";

    public static final String IMAP_FINISHED_JOB_STATE = "finishedJobState";

    public static final String IMAP_FINISHED_JOB_METRICS = "finishedJobMetrics";

    public static final String IMAP_STATE_TIMESTAMPS = "stateTimestamps";

    public static final String IMAP_OWNED_SLOT_PROFILES = "ownedSlotProfilesIMap";

    public static final String IMAP_RESOURCE_MANAGER_REGISTER_WORKER = "ResourceManager_RegisterWorker";
}
