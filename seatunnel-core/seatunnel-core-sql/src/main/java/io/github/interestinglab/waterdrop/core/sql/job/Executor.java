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

package io.github.interestinglab.waterdrop.core.sql.job;

import io.github.interestinglab.waterdrop.core.sql.splitter.SqlStatementSplitter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.util.List;

public class Executor {

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

        List<String> stmts = SqlStatementSplitter.normalizeStatements(workFlowContent);
        for (String stmt : stmts) {
            Operation op = stEnv.getParser().parse(stmt).get(0);
            if (op instanceof CatalogSinkModifyOperation) {
                statementSet.addInsertSql(stmt);
            } else {
                tEnv.executeSql(stmt);
            }
        }
        return statementSet;
    }

}
