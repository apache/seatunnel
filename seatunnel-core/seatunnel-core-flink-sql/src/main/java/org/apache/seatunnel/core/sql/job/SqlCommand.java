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

import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

public enum SqlCommand {

    CACHE_INTERVAL_JOIN("(CREATE\\s+VIEW.*/\\*\\+\\s*OPTIONS\\s*\\([a-zA-Z0-9'.,_\\s=-]*'last\\.cache\\.ttl'\\s*=\\s*'\\d+'[a-zA-Z0-9'.,_\\s=-]*\\)\\s*\\*/.*)", operands -> Optional.of(new String[]{operands[0]})),
    INSERT_INTO("(INSERT\\s+INTO.*)", operands -> Optional.of(new String[]{operands[0]})),
    INSERT_OVERWRITE("(INSERT\\s+OVERWRITE.*)", operands -> Optional.of(new String[]{operands[0]})),
    CREATE_TABLE("(CREATE\\s+TABLE.*)", operands -> Optional.of(new String[]{operands[0]})),
    CREATE_FUNCTION("(CREATE\\s+(TEMPORARY)*\\s*FUNCTION.*)", operands -> Optional.of(new String[]{operands[0]})),
    CREATE_VIEW("(CREATE\\s+VIEW.*)", operands -> Optional.of(new String[]{operands[0]})),
    USE("(USE\\s+(?!CATALOG)(.*))", operands -> Optional.of(new String[]{operands[0]})),
    USE_CATALOG("(USE\\s+CATALOG.*)", operands -> Optional.of(new String[]{operands[0]})),
    DROP_TABLE("(DROP\\s+TABLE.*)", operands -> Optional.of(new String[]{operands[0]})),
    DROP_DATABASE("(DROP\\s+DATABASE.*)", operands -> Optional.of(new String[]{operands[0]})),
    DROP_VIEW("(DROP\\s+VIEW.*)", operands -> Optional.of(new String[]{operands[0]})),
    DROP_FUNCTION("(DROP\\s+FUNCTION.*)", operands -> Optional.of(new String[]{operands[0]})),
    ALTER_TABLE("(ALTER\\s+TABLE.*)", operands -> Optional.of(new String[]{operands[0]})),
    ALTER_DATABASE("(ALTER\\s+DATABASE.*)", operands -> Optional.of(new String[]{operands[0]})),
    ALTER_FUNCTION("(ALTER\\s+FUNCTION.*)", operands -> Optional.of(new String[]{operands[0]})),
    SELECT("(WITH.*SELECT.*|SELECT.*)", operands -> Optional.of(new String[]{operands[0]})),
    SHOW_CATALOGS("SHOW\\s+CATALOGS", operands -> Optional.of(new String[]{"SHOW CATALOGS"})),
    SHOW_DATABASES("SHOW\\s+DATABASES", operands -> Optional.of(new String[]{"SHOW DATABASES"})),
    SHOW_TABLES("SHOW\\s+TABLES", operands -> Optional.of(new String[]{"SHOW TABLES"})),
    SHOW_FUNCTIONS("SHOW\\s+FUNCTIONS", operands -> Optional.of(new String[]{"SHOW FUNCTIONS"})),
    SHOW_MODULES("SHOW\\s+MODULES", operands -> Optional.of(new String[]{"SHOW MODULES"})),
    CREATE_CATALOG("(CREATE\\s+CATALOG.*)", operands -> Optional.of(new String[]{operands[0]})),
    SET("SET(\\s+(\\S+)\\s*=(.*))?", operands -> {
        if (operands.length >= FlinkSqlConstant.FLINK_SQL_SET_OPERANDS) {
            if (operands[0] == null) {
                return Optional.of(new String[0]);
            }
        } else {
            return Optional.empty();
        }
        return Optional.of(new String[]{operands[1], operands[2]});
    });

    public final Pattern pattern;

    public final Function<String[], Optional<String[]>> operandConverter;

    public Pattern getPattern() {
        return pattern;
    }

    public Function<String[], Optional<String[]>> getOperandConverter() {
        return operandConverter;
    }

    SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
        this.pattern = Pattern.compile(matchingRegex, FlinkSqlConstant.DEFAULT_PATTERN_FLAGS);
        this.operandConverter = operandConverter;
    }

}
