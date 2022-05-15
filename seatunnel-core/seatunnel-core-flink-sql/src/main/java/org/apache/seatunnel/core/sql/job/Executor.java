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

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.core.sql.classloader.CustomClassLoader;
import org.apache.seatunnel.core.sql.splitter.SqlStatementSplitter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Executor.class);

    private static final String FLINK_SQL_SET_MATCHING_REGEX = "SET(\\s+(\\S+)\\s*=(.*))?";
    private static final int FLINK_SQL_SET_OPERANDS = 3;

    private static CustomClassLoader CLASSLOADER = new CustomClassLoader();

    private static final String CONNECTOR_IDENTIFIER = "connector";
    private static final String SQL_CONNECTOR_PREFIX = "flink-sql";
    private static final String CONNECTOR_JAR_PREFIX = "flink-sql-connector-";

    private Executor() {
        throw new IllegalStateException("Utility class");
    }

    public static void runJob(JobInfo jobInfo) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        final Configuration executionEnvConfiguration;
        try {
            executionEnvConfiguration =
                  (Configuration) Objects.requireNonNull(ReflectionUtils.getDeclaredMethod(StreamExecutionEnvironment.class,
                    "getConfiguration")).orElseThrow(() -> new RuntimeException("can't find " +
                    "method: getConfiguration")).invoke(env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(CLASSLOADER);

            StatementSet statementSet = handleStatements(jobInfo.getJobContent(), tEnv, executionEnvConfiguration);
            statementSet.execute();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    /**
     * Handle each statement.
     */
    private static StatementSet handleStatements(String workFlowContent, StreamTableEnvironment tEnv, Configuration executionEnvConfiguration) {

        StatementSet statementSet = tEnv.createStatementSet();
        TableEnvironmentImpl stEnv = (TableEnvironmentImpl) tEnv;
        Configuration configuration = tEnv.getConfig().getConfiguration();

        List<String> stmts = SqlStatementSplitter.normalizeStatements(workFlowContent);
        for (String stmt : stmts) {
            Optional<Pair<String, String>> optional = parseSetOperation(stmt);
            if (optional.isPresent()) {
                Pair<String, String> setOptionPair = optional.get();
                callSetOperation(configuration, setOptionPair.getLeft(), setOptionPair.getRight());
                continue;
            }
            Operation op = stEnv.getParser().parse(stmt).get(0);
            if (op instanceof CatalogSinkModifyOperation) {
                statementSet.addInsertSql(stmt);
            } else {
                if (op instanceof CreateTableOperation) {
                    String connectorType = ((CreateTableOperation) op).getCatalogTable().getOptions().get(CONNECTOR_IDENTIFIER);
                    loadConnector(connectorType, executionEnvConfiguration);
                }

                tEnv.executeSql(stmt);
            }
        }
        return statementSet;
    }

    /*
     * The loadConnector method is best-effort, and it will not throw exception if the connector is not found.
     * If the table declared in 'create table' with connector 'xxx' and the table is not referenced in the job, namely,
     * used in the 'insert into' statement, the connector 'xxx' will not be needed by Flink.
     * So it might be ok fail to load it. If it's needed, we can see the error in Flink logs.
     *
     * Refer https://github.com/apache/incubator-seatunnel/pull/1850
     */
    private static void loadConnector(String connectorType, Configuration configuration) {
        // Handle for two cases:
        // 1. Flink built-in connectors.
        // 2. Connectors have been placed in classpath.
        for (Factory factory : ServiceLoader.load(Factory.class, CLASSLOADER)) {
            if (factory.factoryIdentifier().equals(connectorType)) {
                return;
            }
        }

        Common.setDeployMode(DeployMode.CLIENT);
        File connectorDir = Common.connectorJarDir(SQL_CONNECTOR_PREFIX).toFile();
        if (!connectorDir.exists() || connectorDir.listFiles() == null) {
            return;
        }

        List<File> connectorFiles = Arrays.stream(connectorDir.listFiles())
            .filter(file -> file.getName().startsWith(CONNECTOR_JAR_PREFIX + connectorType))
            .collect(Collectors.toList());

        if (connectorFiles.size() > 1) {
            LOGGER.warn("Found more than one connector jars for {}. Only the first one will be loaded.", connectorType);
        }

        File connectorFile = connectorFiles.size() >= 1 ? connectorFiles.get(0) : null;

        if (connectorFile != null) {
            // handleStatements need this.
            CLASSLOADER.addJar(connectorFile.toPath());

            List<String> jars = configuration.get(PipelineOptions.JARS);
            jars = jars == null ? new ArrayList<>() : jars;

            List<String> classpath = configuration.get(PipelineOptions.CLASSPATHS);
            classpath = classpath == null ? new ArrayList<>() : classpath;

            try {
                String connectorURL = connectorFile.toPath().toUri().toURL().toString();
                jars.add(connectorURL);
                classpath.add(connectorURL);

                configuration.set(PipelineOptions.JARS, jars);
                configuration.set(PipelineOptions.CLASSPATHS, classpath);
            } catch (MalformedURLException ignored) {
                LOGGER.error("Failed to load connector {}. Connector file: {}", connectorType, connectorFile.getAbsolutePath());
            }
        }
    }

    @VisibleForTesting
    static Optional<Pair<String, String>> parseSetOperation(String stmt) {
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

    private static Optional<Pair<String, String>> operandConverter(String[] operands){
        if (operands.length != FLINK_SQL_SET_OPERANDS) {
            return Optional.empty();
        }

        return Optional.of(Pair.of(operands[1].trim(), operands[2].trim()));
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
