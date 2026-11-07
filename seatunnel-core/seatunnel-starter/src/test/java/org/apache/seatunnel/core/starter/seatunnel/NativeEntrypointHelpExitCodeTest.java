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

import org.apache.seatunnel.core.starter.seatunnel.args.ServerCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

import org.junit.jupiter.api.Test;

import static com.github.stefanbirkner.systemlambda.SystemLambda.catchSystemExit;
import static org.apache.seatunnel.core.starter.constants.SeaTunnelStarterConstants.USAGE_EXIT_CODE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A successful help request on a native entrypoint exits with {@code 0}; argument errors keep the
 * usage exit code. The default parser overload must stay on {@code 234} so the Spark and Flink
 * launchers keep their protocol.
 */
class NativeEntrypointHelpExitCodeTest {

    @Test
    void serverHelpExitsZero() throws Exception {
        assertEquals(0, catchSystemExit(() -> SeaTunnelServer.main(new String[] {"--help"})));
    }

    @Test
    void connectorHelpExitsZero() throws Exception {
        assertEquals(0, catchSystemExit(() -> SeaTunnelConnector.main(new String[] {"--help"})));
    }

    @Test
    void metadataExportHelpExitsZero() throws Exception {
        assertEquals(
                0,
                catchSystemExit(
                        () -> SeaTunnelMetadataExporter.main(new String[] {"--help"})));
    }

    @Test
    void defaultParseKeepsUsageExitCode() throws Exception {
        int statusCode =
                catchSystemExit(
                        () ->
                                CommandLineUtils.parse(
                                        new String[] {"--help"},
                                        new ServerCommandArgs(),
                                        "legacy-shell",
                                        true));
        assertEquals(USAGE_EXIT_CODE, statusCode);
    }

    @Test
    void invalidArgumentKeepsUsageExitCode() throws Exception {
        int statusCode =
                catchSystemExit(
                        () ->
                                CommandLineUtils.parse(
                                        new String[] {"-cn"},
                                        new ServerCommandArgs(),
                                        "legacy-shell",
                                        true,
                                        0));
        assertEquals(USAGE_EXIT_CODE, statusCode);
    }
}
