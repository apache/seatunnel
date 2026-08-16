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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** Verifies local file identity across rename and replacement operations. */
class LocalFileIdentityTest {

    @TempDir private Path tempDir;

    @Test
    void testIdentityRemainsStableAcrossRename() throws Exception {
        Path activeFile = tempDir.resolve("application.log");
        Path rotatedFile = tempDir.resolve("application.log.1");
        Files.write(activeFile, "first\n".getBytes());

        String identity = LocalFileIdentity.read(activeFile.toString());
        Files.move(activeFile, rotatedFile);

        Assertions.assertEquals(identity, LocalFileIdentity.read(rotatedFile.toString()));
    }

    @Test
    void testIdentityChangesWhenPathIsReplaced() throws Exception {
        Path activeFile = tempDir.resolve("application.log");
        Path replacementFile = tempDir.resolve("replacement.log");
        Files.write(activeFile, "first\n".getBytes());
        Files.write(replacementFile, "replacement\n".getBytes());

        String identity = LocalFileIdentity.read(activeFile.toString());
        Files.move(replacementFile, activeFile, StandardCopyOption.REPLACE_EXISTING);

        Assertions.assertNotEquals(identity, LocalFileIdentity.read(activeFile.toString()));
    }
}
