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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.FileSinkTransactionFileNameGenerator;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

@RunWith(PowerMockRunner.class)
public class TestFileSinkTransactionFileNameGenerator {

    @Test
    public void testGenerateFileName() {
        FileSinkTransactionFileNameGenerator fileNameGenerator = new FileSinkTransactionFileNameGenerator(FileFormat.TEXT, "test_${transactionId}_${uuid}_${now}", "yyyy.MM.dd");
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy.MM.dd");
        final String formattedDate = df.format(ZonedDateTime.now());
        String fileName = fileNameGenerator.generateFileName("T_12345678_1_0");
        Assert.assertTrue(fileName.startsWith("test_T_12345678_1_0_"));
        Assert.assertTrue(fileName.endsWith(formattedDate + ".txt"));

        fileNameGenerator = new FileSinkTransactionFileNameGenerator(FileFormat.TEXT, null, "yyyy.MM.dd");
        fileName = fileNameGenerator.generateFileName("T_12345678_1_0");
        Assert.assertEquals("T_12345678_1_0.txt", fileName);
    }
}
