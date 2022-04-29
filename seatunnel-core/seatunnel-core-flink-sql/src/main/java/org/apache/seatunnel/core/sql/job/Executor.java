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

import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Executor {

    private Executor() {
        throw new IllegalStateException("Utility class");
    }

    public static void runJob(JobInfo jobInfo) {
        FlinkEnvConfigInfo flinkEnvConfigInfo = new FlinkEnvConfigInfo(jobInfo.getFlinkEnvList());
        FlinkEnvironment flinkEnvironment = new FlinkEnvironment();
        flinkEnvironment.setConfig(flinkEnvConfigInfo.getEnvConfig()).setJobMode(flinkEnvConfigInfo.selectJobMode()).prepare();
        TableEnvironment tEnv;
        if (flinkEnvironment.isStreaming()) {
            tEnv = flinkEnvironment.getStreamTableEnvironment();
            flinkEnvironment.getStreamExecutionEnvironment().registerJobListener(new ApplicationInfoJobListener(flinkEnvironment.getStreamExecutionEnvironment(), null));
        } else {
            tEnv = flinkEnvironment.getBatchTableEnvironment();
            flinkEnvironment.getBatchEnvironment().registerJobListener(new ApplicationInfoJobListener(null, flinkEnvironment.getBatchEnvironment()));
        }
        StatementSet statementSet = handleStatements(jobInfo.getFlinkSqlList(), tEnv);
        TableResult tableResult = statementSet.execute();
        if (tableResult == null || !tableResult.getJobClient().isPresent() || tableResult.getJobClient().get().getJobID() == null) {
            throw new RuntimeException("The task failed to run. The job id was not obtained");
        }
        JobID jobID = tableResult.getJobClient().get().getJobID();
        LogPrint.jobPrint(jobID.toString());
    }

    /**
     * Handle each statement.
     */
    private static StatementSet handleStatements(List<String> flinkSqlList, TableEnvironment tEnv) {
        StatementSet statementSet = tEnv.createStatementSet();
        TableEnvironmentImpl stEnv = (TableEnvironmentImpl) tEnv;
        Configuration configuration = tEnv.getConfig().getConfiguration();
        for (String stmt : flinkSqlList) {
            if (analysisSqlIsSetOperation(stmt)) {
                Optional<SqlCommandCall> optionalCall = parse(stmt);
                optionalCall.ifPresent(sqlCommandCall -> callSetOperation(configuration, sqlCommandCall.getOperands()[0], sqlCommandCall.getOperands()[1]));
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

    private static Boolean analysisSqlIsSetOperation(String stmt) {
        Pattern pattern = SqlCommand.SET.getPattern();
        Matcher matcher = pattern.matcher(stmt);
        return matcher.find() && stmt.trim().toUpperCase().startsWith(FlinkSqlConstant.FLINK_SQL_SET_PREFIX);
    }

    private static Optional<SqlCommandCall> parse(String stmt) {
        stmt = stmt.trim();
        for (SqlCommand cmd : SqlCommand.values()) {
            final Matcher matcher = cmd.getPattern().matcher(stmt);
            if (matcher.matches()) {
                final String[] groups = new String[matcher.groupCount()];
                for (int i = 0; i < groups.length; i++) {
                    groups[i] = matcher.group(i + 1);
                }
                return cmd.getOperandConverter().apply(groups).map(operands -> new SqlCommandCall(cmd, operands));
            }
        }
        return Optional.empty();
    }

    private static void callSetOperation(Configuration configuration, String key, String value) {
        if (StringUtils.isEmpty(key)) {
            new IllegalArgumentException("key can not be empty!");
        }
        if (StringUtils.isEmpty(value)) {
            new IllegalArgumentException("value can not be empty!");
        }
        LogPrint.setSqlPrint(key, value);
        configuration.setString(key, value);
    }

}
