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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class LogPrint {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogPrint.class);

    private static final String TAG_ARGS_PRINT = "print args";

    private static final String TAG_SQL_PRINT = "print sql";

    private static final String TAG_ENV_PRINT = "print env";

    private static final String TAG_JOB_PRINT = "print jobId";

    private static final String TAG_SET_PRINT = "print set";

    public static void argsPrint(String[] args) {
        if (args == null || args.length <= 0) {
            return;
        }
        logPrint(TAG_ARGS_PRINT, args);
    }

    public static void sqlPrint(List<String> sqlList) {
        if (sqlList == null || sqlList.isEmpty()) {
            return;
        }
        logPrint(TAG_SQL_PRINT, sqlList.toArray(new String[]{}));
    }

    public static void envPrint(List<String> envList) {
        if (envList == null || envList.isEmpty()) {
            return;
        }
        logPrint(TAG_ENV_PRINT, envList.toArray(new String[]{}));
    }

    public static void jobPrint(String jobId) {
        String printStr = FlinkSqlConstant.QUERY_JOBID_KEY_WORD + jobId;
        logPrint(TAG_JOB_PRINT, printStr);
    }

    public static void setSqlPrint(String key, String value) {
        String printStr = "key = " + key + " value = " + value;
        logPrint(TAG_SET_PRINT, printStr);
    }

    public static void logPrint(String tag, String... strs) {
        StringBuilder printStringBuilder = new StringBuilder("\n ############# ").append(tag).append(" ############# \n");
        String line = FlinkSqlConstant.SYSTEM_LINE_SEPARATOR;
        if (strs != null && strs.length > 0) {
            Arrays.stream(strs).forEach(str -> printStringBuilder.append(str).append(line));
        }
        String printStr = printStringBuilder.toString();
        LOGGER.info(printStr);
    }

}
