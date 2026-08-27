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

package org.apache.seatunnel.core.starter.command;

import org.apache.seatunnel.common.config.DeployMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ConfDecryptCommandTest {

    public static Path getFilePath(String path) throws URISyntaxException {
        URL resource = ConfDecryptCommandTest.class.getResource(path);
        Assertions.assertNotNull(resource);
        return Paths.get(resource.toURI());
    }

    @Test
    public void testEncrypt() throws URISyntaxException {
        TestCommandArgs testCommandArgs = new TestCommandArgs();
        Path filePath = getFilePath("/shade.conf");
        testCommandArgs.setDecrypt(true);
        testCommandArgs.setConfigFile(filePath.toString());
        ConfDecryptCommand confDecryptCommand = new ConfDecryptCommand(testCommandArgs);
        confDecryptCommand.execute();
    }

    public static class TestCommandArgs extends AbstractCommandArgs {

        @Override
        public DeployMode getDeployMode() {
            return null;
        }

        @Override
        public Command<?> buildCommand() {
            return null;
        }
    }
}
