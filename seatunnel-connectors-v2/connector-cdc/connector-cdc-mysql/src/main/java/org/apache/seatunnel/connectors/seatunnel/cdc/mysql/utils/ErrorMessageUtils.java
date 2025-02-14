/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils;

import java.util.regex.Pattern;

/** This util tries to optimize error message for some exceptions. */
public class ErrorMessageUtils {
    private static final Pattern SERVER_ID_CONFLICT =
            Pattern.compile(
                    ".*A slave with the same server_uuid/server_id as this slave has connected to the master.*");
    private static final Pattern MISSING_BINLOG_POSITION_WHEN_BINLOG_EXPIRE =
            Pattern.compile(
                    ".*The connector is trying to read binlog.*but this is no longer available on the server.*");
    private static final Pattern MISSING_TRANSACTION_WHEN_BINLOG_EXPIRE =
            Pattern.compile(
                    ".*Cannot replicate because the (master|source) purged required binary logs.*");

    /** Add more error details for some exceptions. */
    public static String optimizeErrorMessage(String msg) {
        if (msg == null) {
            return null;
        }
        if (SERVER_ID_CONFLICT.matcher(msg).matches()) {
            // Optimize the error msg when server id conflict
            msg +=
                    "\nThe 'server-id' in the mysql cdc connector should be globally unique, but conflicts happen now.\n"
                            + "The server id conflict may happen in the following situations: \n"
                            + "1. The server id has been used by other mysql cdc table in the current job.\n"
                            + "2. The server id has been used by the mysql cdc table in other jobs.\n"
                            + "3. The server id has been used by other sync tools like canal, debezium and so on.\n";
        } else if (MISSING_BINLOG_POSITION_WHEN_BINLOG_EXPIRE.matcher(msg).matches()
                || MISSING_TRANSACTION_WHEN_BINLOG_EXPIRE.matcher(msg).matches()) {
            // Optimize the error msg when binlog is unavailable
            msg +=
                    "\nThe required binary logs are no longer available on the server. This may happen in following situations:\n"
                            + "1. The speed of CDC source reading is too slow to exceed the binlog expired period. You can consider increasing the binary log expiration period, you can also to check whether there is back pressure in the job and optimize your job.\n"
                            + "2. The job runs normally, but something happens in the database and lead to the binlog cleanup. You can try to check why this cleanup happens from MySQL side.";
        }
        return msg;
    }
}
