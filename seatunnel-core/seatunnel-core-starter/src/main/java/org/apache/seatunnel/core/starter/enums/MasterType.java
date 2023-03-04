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

package org.apache.seatunnel.core.starter.enums;

/** SeaTunnel job submitted master target, works with ST-Engine and Flink engine */
public enum MasterType {
    /** ST Engine */
    LOCAL("local"),
    CLUSTER("cluster"),

    /** Flink run deploy mode */
    REMOTE("remote"),
    YARN_SESSION("yarn-session"),
    YARN_PER_JOB("yarn-per-job"),
    KUBERNETES_SESSION("kubernetes-session"),

    /** Flink run-application deploy mode */
    YARN_APPLICATION("yarn-application"),
    KUBERNETES_APPLICATION("kubernetes-application");

    private final String master;

    MasterType(String master) {
        this.master = master;
    }

    public String getMaster() {
        return master;
    }
}
