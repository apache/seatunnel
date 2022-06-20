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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.FileSinkPartitionDirNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.FileSinkTransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.HdfsTxtTransactionStateFileWriter;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RunWith(JUnit4.class)
public class TestHdfsTxtTransactionStateFileWriter {

    @SuppressWarnings("checkstyle:MagicNumber")
    @Test
    public void testHdfsTextTransactionStateFileWriter() throws Exception {
        String[] fieldNames = new String[]{"c1", "c2", "c3", "c4"};
        SeaTunnelDataType[] seaTunnelDataTypes = new SeaTunnelDataType[]{BasicType.BOOLEAN_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE};
        SeaTunnelRowType seaTunnelRowTypeInfo = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);

        List<Integer> sinkColumnIndexInRow = new ArrayList<>();
        sinkColumnIndexInRow.add(0);
        sinkColumnIndexInRow.add(1);

        List<String> hivePartitionFieldList = new ArrayList<>();
        hivePartitionFieldList.add("c3");
        hivePartitionFieldList.add("c4");

        List<Integer> partitionFieldIndexInRow = new ArrayList<>();
        partitionFieldIndexInRow.add(2);
        partitionFieldIndexInRow.add(3);

        String jobId = System.currentTimeMillis() + "";
        String targetPath = "/tmp/hive/warehouse/seatunnel.db/test1";
        String tmpPath = "/tmp/seatunnel";

        TransactionStateFileWriter fileWriter = new HdfsTxtTransactionStateFileWriter(seaTunnelRowTypeInfo,
            new FileSinkTransactionFileNameGenerator(FileFormat.TEXT, null, "yyyy.MM.dd"),
            new FileSinkPartitionDirNameGenerator(hivePartitionFieldList, partitionFieldIndexInRow, "${k0}=${v0}/${k1}=${v1}"),
            sinkColumnIndexInRow,
            tmpPath,
            targetPath,
            jobId,
            0,
            String.valueOf('\001'),
            "\n");

        String transactionId = fileWriter.beginTransaction(1L);

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(new Object[]{true, 1, "str1", "str2"});
        fileWriter.write(seaTunnelRow);

        SeaTunnelRow seaTunnelRow1 = new SeaTunnelRow(new Object[]{true, 1, "str1", "str3"});
        fileWriter.write(seaTunnelRow1);

        Optional<FileCommitInfo> fileCommitInfoOptional = fileWriter.prepareCommit();
        //check file exists and file content
        Assert.assertTrue(fileCommitInfoOptional.isPresent());
        FileCommitInfo fileCommitInfo = fileCommitInfoOptional.get();
        String transactionDir = tmpPath + "/seatunnel/" + jobId + "/" + transactionId;
        Assert.assertEquals(transactionDir, fileCommitInfo.getTransactionDir());
        Assert.assertEquals(2, fileCommitInfo.getNeedMoveFiles().size());
        Map<String, String> needMoveFiles = fileCommitInfo.getNeedMoveFiles();
        Assert.assertEquals(targetPath + "/c3=str1/c4=str2/" + transactionId + ".txt", needMoveFiles.get(transactionDir + "/c3=str1/c4=str2/" + transactionId + ".txt"));
        Assert.assertEquals(targetPath + "/c3=str1/c4=str3/" + transactionId + ".txt", needMoveFiles.get(transactionDir + "/c3=str1/c4=str3/" + transactionId + ".txt"));
    }
}
