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

package org.apache.seatunnel.connectors.seatunnel.openmldb.config;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

public class OpenMldbSqlExecutor {
    private static final SdkOption SDK_OPTION = new SdkOption();
    private static volatile SqlClusterExecutor SQL_EXECUTOR;

    private OpenMldbSqlExecutor() {

    }

    public static void initSdkOption(OpenMldbParameters openMldbParameters) {
        if (openMldbParameters.getClusterMode()) {
            SDK_OPTION.setZkCluster(openMldbParameters.getZkHost());
            SDK_OPTION.setZkPath(openMldbParameters.getZkPath());
        } else {
            SDK_OPTION.setHost(openMldbParameters.getHost());
            SDK_OPTION.setPort(openMldbParameters.getPort());
            SDK_OPTION.setClusterMode(false);
        }
        SDK_OPTION.setSessionTimeout(openMldbParameters.getSessionTimeout());
        SDK_OPTION.setRequestTimeout(openMldbParameters.getRequestTimeout());
    }

    public static SqlClusterExecutor getSqlExecutor() throws SqlException {
        if (SQL_EXECUTOR == null) {
            synchronized (OpenMldbSqlExecutor.class) {
                if (SQL_EXECUTOR == null) {
                    SQL_EXECUTOR = new SqlClusterExecutor(SDK_OPTION);
                }
            }
        }
        return SQL_EXECUTOR;
    }

    public static void close() {
        if (SQL_EXECUTOR != null) {
            synchronized (OpenMldbParameters.class) {
                if (SQL_EXECUTOR != null) {
                    SQL_EXECUTOR.close();
                    SQL_EXECUTOR = null;
                }
            }
        }
    }
}
