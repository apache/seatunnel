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

package org.apache.seatunnel.resource.yarn.launch;

/** Stable localization names and environment contracts shared by YARN application processes. */
public final class YarnConstants {
    /** Directory under which YARN expands the localized SeaTunnel distribution archive. */
    public static final String LOCALIZED_DISTRIBUTION_NAME = "seatunnel";

    /** Localized application specification read by the ApplicationMaster. */
    public static final String LOCALIZED_SPECIFICATION_NAME = "application.properties";

    /** Localized merged Hadoop configuration used by the master and workers. */
    public static final String LOCALIZED_HADOOP_CONFIG_NAME = "hadoop-conf.xml";

    /** Environment variable carrying the application-owned remote staging directory. */
    public static final String STAGING_DIRECTORY_ENV = "SEATUNNEL_YARN_STAGING";

    /** Environment variables exported to each localized SeaTunnel process. */
    static final String SEATUNNEL_HOME_ENV = "SEATUNNEL_HOME";

    static final String HADOOP_CONF_DIR_ENV = "HADOOP_CONF_DIR";

    /** JVM property used by workers to locate their distribution root. */
    static final String SEATUNNEL_HOME_PROPERTY = "seatunnel.home";

    /** Heap sizing used by both ApplicationMaster and worker JVMs. */
    static final int JVM_HEAP_NUMERATOR = 3;

    static final int JVM_HEAP_DENOMINATOR = 4;
    static final int MINIMUM_JVM_HEAP_MB = 64;

    /** Files and directories relative to the localized SeaTunnel distribution home. */
    static final String LOG4J_CONFIG_FILE = "config/log4j2_client.properties";

    static final String YARN_CLASSPATH =
            ".:%sconfig:%sstarter/seatunnel-starter.jar:%sstarter/logging/*:%slib/*:%sresource-managers/yarn/*";

    private YarnConstants() {}
}
