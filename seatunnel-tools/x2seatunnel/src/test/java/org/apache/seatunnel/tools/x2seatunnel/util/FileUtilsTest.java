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

package org.apache.seatunnel.tools.x2seatunnel.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class FileUtilsTest {

    @Test
    public void testBasicFileOperations() throws IOException {
        String testFile = "target/test-file.txt";
        String testContent = "Hello, World!";

        // 写入文件
        FileUtils.writeFile(testFile, testContent);

        // 验证文件存在
        Assertions.assertTrue(FileUtils.exists(testFile));

        // 读取文件
        String content = FileUtils.readFile(testFile);
        Assertions.assertEquals(testContent, content);

        // 清理
        new File(testFile).delete();
    }
}
