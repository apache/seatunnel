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

import org.apache.seatunnel.core.sql.splitter.SqlStatementSplitter;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Executor {

    private static final String FLINK_SQL_SET_MATCHING_REGEX = "SET(\\s+(\\S+)\\s*=(.*))?";
    private static final int FLINK_SQL_SET_OPERANDS = 3;

    private Executor() {
        throw new IllegalStateException("Utility class");
    }

    public static void runJob(JobInfo jobInfo) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        StatementSet statementSet = handleStatements(jobInfo.getJobContent(), tEnv);
        statementSet.execute();
    }

    /**
     * Handle each statement.
     */
    private static StatementSet handleStatements(String workFlowContent, StreamTableEnvironment tEnv) {

        StatementSet statementSet = tEnv.createStatementSet();
        TableEnvironmentImpl stEnv = (TableEnvironmentImpl) tEnv;
        Configuration configuration = tEnv.getConfig().getConfiguration();

        List<String> stmts = SqlStatementSplitter.normalizeStatements(workFlowContent);
        for (String stmt : stmts) {
            Optional<String[]> optional = setOperationParse(stmt);
            if (optional.isPresent()) {
                String[] setOptionStrs = optional.get();
                callSetOperation(configuration, setOptionStrs[0].trim(), setOptionStrs[1].trim());
                continue;
            }
            Operation op = stEnv.getParser().parse(stmt).get(0);
            if (op instanceof CatalogSinkModifyOperation) {
                statementSet.addInsertSql(stmt);
            } else {
                tEnv.executeSql(stmt);
            }
        }
        return statementSet;
    }

    private static Optional<String[]> setOperationParse(String stmt) {
        stmt = stmt.trim();
        Pattern pattern = Pattern.compile(FLINK_SQL_SET_MATCHING_REGEX, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        final Matcher matcher = pattern.matcher(stmt);
        if (matcher.matches()) {
            final String[] groups = new String[matcher.groupCount()];
            for (int i = 0; i < groups.length; i++) {
                groups[i] = matcher.group(i + 1);
            }
            return operandConverter(groups);
        }
        return Optional.empty();
    }

    private static Optional<String[]> operandConverter(String[] operands){
        if (operands.length >= FLINK_SQL_SET_OPERANDS) {
            if (operands[0] == null) {
                return Optional.of(new String[0]);
            }
        } else {
            return Optional.empty();
        }
        return Optional.of(new String[]{operands[1], operands[2]});
    }

    private static void callSetOperation(Configuration configuration, String key, String value) {
        if (StringUtils.isEmpty(key)) {
            new IllegalArgumentException("key can not be empty!");
        }
        if (StringUtils.isEmpty(value)) {
            new IllegalArgumentException("value can not be empty!");
        }
        configuration.setString(key, value);
    }

}
