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

package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;

class HadoopFileSystemProxyDeleteEmptyDirectoryTest {

    @Test
    void shouldDeleteEmptyDirectory() throws Exception {
        Path emptyDir = new Path("/tmp/empty");
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(fs.exists(emptyDir)).thenReturn(true);
        Mockito.when(fs.getFileStatus(emptyDir)).thenReturn(directoryStatus(emptyDir));
        Mockito.when(fs.listStatus(emptyDir)).thenReturn(new FileStatus[0]);
        Mockito.when(fs.delete(emptyDir, false)).thenReturn(true);

        try (HadoopFileSystemProxy proxy = newProxy(fs)) {
            Assertions.assertTrue(proxy.deleteEmptyDirectory(emptyDir.toUri().toString()));
        }

        Mockito.verify(fs).delete(emptyDir, false);
    }

    @Test
    void shouldSkipMissingFileAndNonEmptyDirectory() throws Exception {
        Path missingPath = new Path("/tmp/missing");
        Path regularFile = new Path("/tmp/data.txt");
        Path nonEmptyDir = new Path("/tmp/non-empty");
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(fs.exists(missingPath)).thenReturn(false);
        Mockito.when(fs.exists(regularFile)).thenReturn(true);
        Mockito.when(fs.getFileStatus(regularFile)).thenReturn(fileStatus(regularFile));
        Mockito.when(fs.exists(nonEmptyDir)).thenReturn(true);
        Mockito.when(fs.getFileStatus(nonEmptyDir)).thenReturn(directoryStatus(nonEmptyDir));
        Mockito.when(fs.listStatus(nonEmptyDir))
                .thenReturn(new FileStatus[] {fileStatus(new Path(nonEmptyDir, "data.txt"))});

        try (HadoopFileSystemProxy proxy = newProxy(fs)) {
            Assertions.assertFalse(proxy.deleteEmptyDirectory(missingPath.toString()));
            Assertions.assertFalse(proxy.deleteEmptyDirectory(regularFile.toString()));
            Assertions.assertFalse(proxy.deleteEmptyDirectory(nonEmptyDir.toUri().toString()));
        }

        Mockito.verify(fs, Mockito.never()).delete(Mockito.any(Path.class), Mockito.anyBoolean());
    }

    @Test
    void shouldIgnoreDirectoryChangedDuringEmptyDirectoryCheck() throws Exception {
        Path changingDir = new Path("/tmp/changing");
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(fs.exists(changingDir)).thenReturn(true);
        Mockito.when(fs.getFileStatus(changingDir)).thenReturn(directoryStatus(changingDir));
        Mockito.when(fs.listStatus(changingDir)).thenThrow(new IOException("changed"));

        try (HadoopFileSystemProxy proxy = newProxy(fs)) {
            Assertions.assertFalse(proxy.deleteEmptyDirectory(changingDir.toUri().toString()));
        }

        Mockito.verify(fs, Mockito.never()).delete(Mockito.any(Path.class), Mockito.anyBoolean());
    }

    private static FileStatus directoryStatus(Path path) {
        return new FileStatus(0, true, 1, 0, 0, path);
    }

    private static FileStatus fileStatus(Path path) {
        return new FileStatus(1, false, 1, 0, 0, path);
    }

    private static HadoopFileSystemProxy newProxy(FileSystem fs) throws Exception {
        HadoopFileSystemProxy proxy =
                Mockito.mock(HadoopFileSystemProxy.class, Mockito.CALLS_REAL_METHODS);
        set(proxy, "fileSystem", fs);
        set(proxy, "isAuthTypeKerberos", false);
        return proxy;
    }

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = null;
        Class<?> cls = target.getClass();
        while (cls != null) {
            try {
                f = cls.getDeclaredField(field);
                break;
            } catch (NoSuchFieldException ignore) {
                cls = cls.getSuperclass();
            }
        }
        if (f == null) {
            throw new NoSuchFieldException(field);
        }
        f.setAccessible(true);
        f.set(target, value);
    }
}
