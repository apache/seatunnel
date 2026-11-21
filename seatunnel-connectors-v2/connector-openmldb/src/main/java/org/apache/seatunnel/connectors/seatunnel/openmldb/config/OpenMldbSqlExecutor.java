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

import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.SQLRouterOptions;
import com._4paradigm.openmldb.StandaloneOptions;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com._4paradigm.openmldb.sql_router_sdk;

public class OpenMldbSqlExecutor {
    private OpenMldbSqlExecutor() {}

    /** Creates an executor owned by a single schema discovery operation. */
    public static SqlClusterExecutor create(OpenMldbParameters openMldbParameters)
            throws SqlException {
        return new SqlClusterExecutor(options(openMldbParameters));
    }

    /** Creates a reader-owned router to access the result set's native null checks. */
    public static SQLRouter createReader(OpenMldbParameters parameters) throws SqlException {
        SqlClusterExecutor.initJavaSdkLibrary("sql_jsdk");
        SdkOption option = options(parameters);
        SQLRouter router;
        if (option.isClusterMode()) {
            SQLRouterOptions nativeOptions = option.buildSQLRouterOptions();
            try {
                router = sql_router_sdk.NewClusterSQLRouter(nativeOptions);
            } finally {
                nativeOptions.delete();
            }
        } else {
            StandaloneOptions nativeOptions = option.buildStandaloneOptions();
            try {
                router = sql_router_sdk.NewStandaloneSQLRouter(nativeOptions);
            } finally {
                nativeOptions.delete();
            }
        }
        if (router == null) {
            throw new SqlException("Failed to create OpenMldb reader");
        }
        return router;
    }

    private static SdkOption options(OpenMldbParameters openMldbParameters) {
        SdkOption sdkOption = new SdkOption();
        if (openMldbParameters.getClusterMode()) {
            sdkOption.setZkCluster(openMldbParameters.getZkHost());
            sdkOption.setZkPath(openMldbParameters.getZkPath());
        } else {
            sdkOption.setHost(openMldbParameters.getHost());
            sdkOption.setPort(openMldbParameters.getPort());
            sdkOption.setClusterMode(false);
        }
        sdkOption.setSessionTimeout(openMldbParameters.getSessionTimeout());
        sdkOption.setRequestTimeout(openMldbParameters.getRequestTimeout());
        return sdkOption;
    }
}
