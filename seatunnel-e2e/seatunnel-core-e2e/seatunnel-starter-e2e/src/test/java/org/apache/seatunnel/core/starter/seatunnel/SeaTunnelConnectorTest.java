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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceFactory;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.transform.sql.SQLTransformFactory;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Only support for seatunnel")
@DisabledOnOs(OS.WINDOWS)
@Slf4j
public class SeaTunnelConnectorTest extends TestSuiteBase implements TestResource {

    /**
     * Connectors that do not implement the Factory interface should be excluded because they cannot
     * be discovered by seatunnel-plugin-discovery todo: If these connectors implement the Factory
     * interface in the future, it should be removed from here
     */
    private static final Set<String> EXCLUDE_CONNECTOR =
            new HashSet() {
                {
                    add("TDengine");
                    add("SelectDBCloud");
                }
            };

    /** All supported transforms. */
    private static final Set<String> TRANSFORMS =
            new HashSet() {
                {
                    add("Copy");
                    add("FieldMapper");
                    add("Filter");
                    add("FilterRowKind");
                    add("JsonPath");
                    add("Replace");
                    add("Split");
                    add("Sql");
                }
            };

    // Match paimon source and paimon sink
    private static final Pattern PATTERN1 =
            Pattern.compile(
                    "(Paimon (source|sink))(.*?)(?=(Paimon (source|sink)|$))", Pattern.DOTALL);
    // Match required options and optional options
    private static final Pattern PATTERN2 =
            Pattern.compile("Required Options:(.*?)(?:Optional Options: (.*?))?$", Pattern.DOTALL);

    @Override
    public void startUp() throws Exception {}

    @Override
    public void tearDown() throws Exception {}

    @TestTemplate
    public void testExecCheck(TestContainer container) throws Exception {
        String[] case1 = {"-l"};
        Container.ExecResult execResult = execCommand(container, case1);
        checkResultForCase1(execResult);

        String[] case2 = {"-l -pt source"};
        execCheck(container, case2, PluginType.SOURCE);

        String[] case3 = {"-l -pt sink"};
        execCheck(container, case3, PluginType.SINK);

        String[] case4 = {"-o Paimon"};
        Container.ExecResult execResult4 = execCommand(container, case4);
        checkStdOutForOptionRule(execResult4.getStdout());

        String[] case5 = {"-o Paimon -pt source"};
        Container.ExecResult execResult5 = execCommand(container, case5);
        checkStdOutForOptionRuleOfSinglePluginTypeWithConnector(execResult5.getStdout());

        String[] case6 = {"-o Paimon -pt sink"};
        Container.ExecResult execResult6 = execCommand(container, case6);
        checkStdOutForOptionRuleOfSinglePluginTypeWithConnector(execResult6.getStdout());

        String[] case7 = {"-o sql -pt transform"};
        Container.ExecResult execResult7 = execCommand(container, case7);
        checkStdOutForOptionRuleOfSinglePluginTypeWithTransform(
                execResult7.getStdout(), new SQLTransformFactory());
    }

    private void checkStdOutForOptionRule(String stdout) {
        Matcher matcher1 = PATTERN1.matcher(stdout.trim());
        String paimonSourceContent = StringUtils.EMPTY;
        String paimonSinkContent = StringUtils.EMPTY;
        Assertions.assertTrue(matcher1.groupCount() >= 3);
        while (matcher1.find()) {
            String type = matcher1.group(2).trim();
            if (type.equals(PluginType.SOURCE.getType())) {
                paimonSourceContent = matcher1.group(3).trim();
            }
            if (type.equals(PluginType.SINK.getType())) {
                paimonSinkContent = matcher1.group(3).trim();
            }
        }
        Assertions.assertTrue(StringUtils.isNoneBlank(paimonSourceContent));
        Assertions.assertTrue(StringUtils.isNoneBlank(paimonSinkContent));
        checkOptionRuleOfSinglePluginType(new PaimonSourceFactory(), paimonSourceContent);
        checkOptionRuleOfSinglePluginType(new PaimonSinkFactory(), paimonSinkContent);
    }

    private void checkStdOutForOptionRuleOfSinglePluginTypeWithTransform(
            String stdout, Factory factory) {
        Matcher matcher2 = PATTERN2.matcher(stdout.trim());
        Assertions.assertTrue(matcher2.find());
        Assertions.assertTrue(matcher2.groupCount() >= 2);
        OptionRule optionRule = factory.optionRule();
        List<Option<?>> exceptRequiredOptions =
                optionRule.getRequiredOptions().stream()
                        .flatMap(requiredOption -> requiredOption.getOptions().stream())
                        .collect(Collectors.toList());
        String requiredOptions = matcher2.group(1).trim();
        String optionalOptions = matcher2.group(2);
        Assertions.assertEquals(
                exceptRequiredOptions.size(),
                (int)
                        Arrays.stream(requiredOptions.split(StringUtils.LF))
                                .map(String::trim)
                                // remove empty string with time
                                .filter(s -> !s.isEmpty())
                                .count());
        Assertions.assertEquals(
                optionRule.getOptionalOptions().size(),
                StringUtils.isBlank(optionalOptions)
                        ? 0
                        : optionalOptions.trim().split(StringUtils.LF).length);
    }

    private void checkStdOutForOptionRuleOfSinglePluginTypeWithConnector(String stdout) {
        Matcher matcher1 = PATTERN1.matcher(stdout.trim());
        Assertions.assertTrue(matcher1.find());
        Assertions.assertTrue(matcher1.groupCount() >= 3);
        String paimonPluginContent = matcher1.group(3).trim();
        Assertions.assertTrue(StringUtils.isNoneBlank(paimonPluginContent));
        String type = matcher1.group(2).trim();
        if (type.equals(PluginType.SOURCE.getType())) {
            checkOptionRuleOfSinglePluginType(new PaimonSourceFactory(), paimonPluginContent);
        } else if (type.equals(PluginType.SINK.getType())) {
            checkOptionRuleOfSinglePluginType(new PaimonSinkFactory(), paimonPluginContent);
        }
    }

    private void checkOptionRuleOfSinglePluginType(Factory factory, String optionRules) {
        Matcher matcher2 = PATTERN2.matcher(optionRules);
        Assertions.assertTrue(matcher2.find());
        Assertions.assertTrue(matcher2.groupCount() >= 2);
        String requiredOptions = matcher2.group(1).trim();
        String optionalOptions = matcher2.group(2).trim();
        Assertions.assertTrue(StringUtils.isNoneBlank(requiredOptions));
        Assertions.assertTrue(StringUtils.isNoneBlank(optionalOptions));
        OptionRule optionRule = factory.optionRule();
        List<Option<?>> exceptRequiredOptions =
                optionRule.getRequiredOptions().stream()
                        .flatMap(requiredOption -> requiredOption.getOptions().stream())
                        .collect(Collectors.toList());
        Assertions.assertEquals(
                exceptRequiredOptions.size(), requiredOptions.split(StringUtils.LF).length);
        Assertions.assertEquals(
                optionRule.getOptionalOptions().size(),
                StringUtils.isBlank(optionalOptions)
                        ? 0
                        : optionalOptions.trim().split(StringUtils.LF).length);
    }

    private void checkResultForCase1(Container.ExecResult execResult) {
        String[] lines = execResult.getStdout().trim().split(StringUtils.LF);
        String sourcesStr = StringUtils.EMPTY;
        String sinkStr = StringUtils.EMPTY;
        String transformStr = StringUtils.EMPTY;
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].equalsIgnoreCase(PluginType.SOURCE.getType())) {
                sourcesStr =
                        StringUtils.capitalize(PluginType.SOURCE.getType())
                                + StringUtils.LF
                                + lines[i + 1];
            } else if (lines[i].equalsIgnoreCase(PluginType.SINK.getType())) {
                sinkStr =
                        StringUtils.capitalize(PluginType.SINK.getType())
                                + StringUtils.LF
                                + lines[i + 1];
            } else if (lines[i].equalsIgnoreCase(PluginType.TRANSFORM.getType())) {
                transformStr =
                        StringUtils.capitalize(PluginType.TRANSFORM.getType())
                                + StringUtils.LF
                                + lines[i + 1];
            }
        }
        Assertions.assertTrue(StringUtils.isNoneBlank(sourcesStr));
        Assertions.assertTrue(StringUtils.isNoneBlank(sinkStr));
        Assertions.assertTrue(StringUtils.isNoneBlank(transformStr));
        checkStdOutForSinglePluginTypeOfConnector(PluginType.SOURCE, sourcesStr);
        checkStdOutForSinglePluginTypeOfConnector(PluginType.SINK, sinkStr);
        checkStdOutForSinglePluginTypeOfTransform(PluginType.TRANSFORM, transformStr);
    }

    private void checkStdOutForSinglePluginTypeOfTransform(PluginType pluginType, String stdOut) {
        Set<String> transforms = getPluginIdentifiers(pluginType, stdOut);
        Assertions.assertTrue(!transforms.isEmpty());
        Set<String> diff =
                TRANSFORMS.stream()
                        .filter(
                                connectorIdentifierStr ->
                                        !transforms.contains(connectorIdentifierStr.toLowerCase()))
                        .collect(Collectors.toSet());
        Assertions.assertTrue(diff.isEmpty());
    }

    private Set<String> getPluginIdentifiers(PluginType pluginType, String stdOut) {
        Set<String> transforms =
                new TreeSet<>(
                        Arrays.asList(
                                stdOut.trim()
                                        .replaceFirst(
                                                StringUtils.capitalize(pluginType.getType()),
                                                StringUtils.EMPTY)
                                        .trim()
                                        .toLowerCase()
                                        .split(StringUtils.SPACE)));
        return transforms;
    }

    private Container.ExecResult execCommand(TestContainer container, String[] case1)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeConnectorCheck(case1);
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertTrue(StringUtils.isBlank(execResult.getStderr()));
        log.info(execResult.getStdout());
        return execResult;
    }

    private void execCheck(TestContainer container, String[] args, PluginType pluginType)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = execCommand(container, args);
        checkStdOutForSinglePluginTypeOfConnector(pluginType, execResult.getStdout());
    }

    private void checkStdOutForSinglePluginTypeOfConnector(PluginType pluginType, String stdOut) {
        Set<String> connectorIdentifier =
                ContainerUtil.getConnectorIdentifier("seatunnel", pluginType.getType()).stream()
                        .filter(connectorIdenf -> !EXCLUDE_CONNECTOR.contains(connectorIdenf))
                        .collect(Collectors.toSet());
        Set<String> connectors = getPluginIdentifiers(pluginType, stdOut);
        Assertions.assertTrue(!connectors.isEmpty());
        // check size
        Assertions.assertEquals(connectorIdentifier.size(), connectors.size());
        Set<String> diff =
                connectorIdentifier.stream()
                        .filter(
                                connectorIdentifierStr ->
                                        !connectors.contains(connectorIdentifierStr.toLowerCase()))
                        .collect(Collectors.toSet());
        // check equals
        Assertions.assertTrue(diff.isEmpty());
    }
}
