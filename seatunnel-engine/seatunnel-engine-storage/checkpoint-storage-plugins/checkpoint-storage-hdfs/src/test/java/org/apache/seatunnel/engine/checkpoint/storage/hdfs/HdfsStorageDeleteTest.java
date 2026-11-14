/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class HdfsStorageDeleteTest {

    @Test
    public void testBatchDeletePropagatesFailure() throws Exception {
        HdfsStorage storage = Mockito.mock(HdfsStorage.class, Mockito.CALLS_REAL_METHODS);
        storage.fs = Mockito.mock(FileSystem.class);
        Mockito.doReturn(Collections.singletonList("1-1-1-1.ser"))
                .when(storage)
                .getFileNames(Mockito.anyString());
        Mockito.when(storage.fs.delete(Mockito.any(Path.class), Mockito.eq(false)))
                .thenReturn(false);

        Assertions.assertThrows(
                CheckpointStorageException.class,
                () -> storage.deleteCheckpoint("1", "1", Collections.singletonList("1")));
    }
}
