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

package org.apache.seatunnel.connectors.seatunnel.file.util;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@DisabledOnOs(OS.WINDOWS)
public class FileSystemUtilsTest {

    private final HadoopFileSystemProxy fileSystemUtils =
            new HadoopFileSystemProxy(new HadoopConf(FS_DEFAULT_NAME_DEFAULT));

    @Test
    void testWithExpectedException() throws IOException {
        fileSystemUtils.deleteFile("/tmp/notfound/test.txt");
        fileSystemUtils.createFile("/tmp/notfound/test.txt");
        // create an existed file will throw exception
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> fileSystemUtils.createFile("/tmp/notfound/test.txt"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-01], ErrorDescription:[SeaTunnel create file '/tmp/notfound/test.txt' failed.]",
                exception.getMessage());
    }
}
