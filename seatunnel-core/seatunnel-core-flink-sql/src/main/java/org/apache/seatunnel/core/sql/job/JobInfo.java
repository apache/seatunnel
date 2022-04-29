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

import org.apache.seatunnel.common.utils.VariablesSubstitute;
import org.apache.seatunnel.core.sql.splitter.SqlStatementSplitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JobInfo {

    private static final String DELIMITER = "=";

    private String jobContent;

    private List<String> flinkEnvList = new ArrayList<>();

    private List<String> flinkSqlList = new ArrayList<>();

    public JobInfo(String jobContent) {
        this.jobContent = jobContent;
    }

    public String getJobContent() {
        return jobContent;
    }

    public List<String> getFlinkEnvList() {
        return flinkEnvList;
    }

    public List<String> getFlinkSqlList() {
        return flinkSqlList;
    }

    public void substitute(List<String> variables) {
        Map<String, String> substituteMap = variables.stream()
                .filter(v -> v.contains(DELIMITER))
                .collect(Collectors.toMap(v -> v.split(DELIMITER)[0], v -> v.split(DELIMITER)[1]));
        jobContent = VariablesSubstitute.substitute(jobContent, substituteMap);
        this.analysisJobContent();
    }

    private void analysisJobContent() {
        List<String> stmts = SqlStatementSplitter.normalizeStatements(this.jobContent);
        for (String stmt : stmts) {
            String patternStr = FlinkSqlConstant.PATTERN_FLINK_ENV_REGEX;
            Pattern pattern = Pattern.compile(patternStr, FlinkSqlConstant.DEFAULT_PATTERN_FLAGS);
            Matcher matcher = pattern.matcher(stmt);
            if (matcher.find() && stmt.trim().toUpperCase().startsWith(FlinkSqlConstant.FLINK_SQL_SET_PREFIX)) {
                String replaceStr = matcher.replaceAll("");
                flinkEnvList.add(replaceStr);
            } else {
                flinkSqlList.add(stmt);
            }
        }
        LogPrint.envPrint(flinkEnvList);
        LogPrint.sqlPrint(flinkSqlList);
    }

}
