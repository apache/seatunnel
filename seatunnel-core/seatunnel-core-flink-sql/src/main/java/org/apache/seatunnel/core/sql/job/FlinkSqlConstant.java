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

import java.util.regex.Pattern;

public final class FlinkSqlConstant {

    public static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

    public static final int FLINK_SQL_SET_OPERANDS = 3;

    public static final String QUERY_JOBID_KEY_WORD = "job-submitted-success:";

    public static final String SYSTEM_LINE_SEPARATOR = System.getProperty("line.separator");

    public static final String PATTERN_FLINK_ENV_REGEX = "SET\\s+flink_env\\.";

    public static final String FLINK_SQL_SET_PREFIX = "SET";

    private FlinkSqlConstant() { }
}
