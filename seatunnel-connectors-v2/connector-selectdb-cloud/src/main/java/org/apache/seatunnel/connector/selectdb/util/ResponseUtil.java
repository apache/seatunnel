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

package org.apache.seatunnel.connector.selectdb.util;

import java.util.regex.Pattern;

/** util for handle response. */
public class ResponseUtil {
    public static final Pattern LABEL_EXIST_PATTERN =
            Pattern.compile(
                    "errCode = 2, detailMessage = Label \\[(.*)\\] "
                            + "has already been used, relate to txn \\[(\\d+)\\]");
    public static final Pattern COMMITTED_PATTERN =
            Pattern.compile(
                    "errCode = 2, detailMessage = No files can be copied, matched (\\d+) files, "
                            + "filtered (\\d+) files because files may be loading or loaded");

    public static final String RETRY_COMMIT =
            "submit task failed, queue size is full: SQL submitter with block policy";

    public static boolean isCommitted(String msg) {
        return COMMITTED_PATTERN.matcher(msg).matches();
    }

    public static boolean needRetryCommit(String msg) {
        return RETRY_COMMIT.equals(msg);
    }
}
