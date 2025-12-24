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

package org.apache.seatunnel.connectors.seatunnel.file.sftp.system;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SftpFileSystemTest {

    @Test
    void convertAllTypeFileName() {
        SFTPFileSystem sftpFileSystem = new SFTPFileSystem();
        Assertions.assertEquals(
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e2e.txt",
                sftpFileSystem.quote("/home/seatunnel/tmp/seatunnel/read/wildcard/e2e.txt"));
        // test file name with wildcard '*'
        Assertions.assertEquals(
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e\\*e.txt",
                sftpFileSystem.quote("/home/seatunnel/tmp/seatunnel/read/wildcard/e*e.txt"));

        // test file name with wildcard '?'
        Assertions.assertEquals(
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e\\?e.txt",
                sftpFileSystem.quote("/home/seatunnel/tmp/seatunnel/read/wildcard/e?e.txt"));
    }
}
