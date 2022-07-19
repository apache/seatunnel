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

package org.apache.seatunnel.connectors.seatunnel.file.sink.hdfs;

import org.apache.seatunnel.connectors.seatunnel.file.sink.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileSinkAggregatedCommitter;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class FileSinkAggregatedCommitterTest {
    @SuppressWarnings("checkstyle:UnnecessaryParentheses")
    @Test
    public void testCommit() throws Exception {
        FileSinkAggregatedCommitter fileSinkAggregatedCommitter = new FileSinkAggregatedCommitter(new HdfsFileSystemCommitter());
        Map<String, Map<String, String>> transactionFiles = new HashMap<>();
        Random random = new Random();
        Long jobId = random.nextLong();
        String transactionDir = String.format("/tmp/seatunnel/seatunnel/%s/T_%s_0_1", jobId, jobId);
        String targetDir = String.format("/tmp/hive/warehouse/%s", jobId);
        Map<String, String> needMoveFiles = new HashMap<>();
        needMoveFiles.put(transactionDir + "/c3=4/c4=rrr/test1.txt", targetDir + "/c3=4/c4=rrr/test1.txt");
        needMoveFiles.put(transactionDir + "/c3=4/c4=bbb/test1.txt", targetDir + "/c3=4/c4=bbb/test1.txt");
        HdfsUtils.createFile(transactionDir + "/c3=4/c4=rrr/test1.txt");
        HdfsUtils.createFile(transactionDir + "/c3=4/c4=bbb/test1.txt");

        transactionFiles.put(transactionDir, needMoveFiles);

        Map<String, List<String>> partitionDirAndVals = new HashMap<>();
        partitionDirAndVals.put("/c3=4/c4=rrr", Arrays.stream((new String[]{"4", "rrr"})).collect(Collectors.toList()));
        partitionDirAndVals.put("/c3=4/c4=bbb", Arrays.stream((new String[]{"4", "bbb"})).collect(Collectors.toList()));

        FileAggregatedCommitInfo fileAggregatedCommitInfo = new FileAggregatedCommitInfo(transactionFiles, partitionDirAndVals);
        List<FileAggregatedCommitInfo> fileAggregatedCommitInfoList = new ArrayList<>();
        fileAggregatedCommitInfoList.add(fileAggregatedCommitInfo);
        fileSinkAggregatedCommitter.commit(fileAggregatedCommitInfoList);

        Assert.assertTrue(HdfsUtils.fileExist(targetDir + "/c3=4/c4=bbb/test1.txt"));
        Assert.assertTrue(HdfsUtils.fileExist(targetDir + "/c3=4/c4=rrr/test1.txt"));
        Assert.assertTrue(!HdfsUtils.fileExist(transactionDir));
    }

    @SuppressWarnings("checkstyle:UnnecessaryParentheses")
    @Test
    public void testCombine() throws Exception {
        FileSinkAggregatedCommitter fileSinkAggregatedCommitter = new FileSinkAggregatedCommitter(new HdfsFileSystemCommitter());
        Map<String, Map<String, String>> transactionFiles = new HashMap<>();
        Random random = new Random();
        Long jobId = random.nextLong();
        String transactionDir = String.format("/tmp/seatunnel/seatunnel/%s/T_%s_0_1", jobId, jobId);
        String targetDir = String.format("/tmp/hive/warehouse/%s", jobId);
        Map<String, String> needMoveFiles = new HashMap<>();
        needMoveFiles.put(transactionDir + "/c3=3/c4=rrr/test1.txt", targetDir + "/c3=3/c4=rrr/test1.txt");
        needMoveFiles.put(transactionDir + "/c3=4/c4=bbb/test1.txt", targetDir + "/c3=4/c4=bbb/test1.txt");
        Map<String, List<String>> partitionDirAndVals = new HashMap<>();
        partitionDirAndVals.put("/c3=3/c4=rrr", Arrays.stream((new String[]{"3", "rrr"})).collect(Collectors.toList()));
        partitionDirAndVals.put("/c3=4/c4=bbb", Arrays.stream((new String[]{"4", "bbb"})).collect(Collectors.toList()));
        FileCommitInfo fileCommitInfo = new FileCommitInfo(needMoveFiles, partitionDirAndVals, transactionDir);
        HdfsUtils.createFile(transactionDir + "/c3=3/c4=rrr/test1.txt");
        HdfsUtils.createFile(transactionDir + "/c3=4/c4=bbb/test1.txt");

        Map<String, String> needMoveFiles1 = new HashMap<>();
        needMoveFiles1.put(transactionDir + "/c3=4/c4=rrr/test2.txt", targetDir + "/c3=4/c4=rrr/test2.txt");
        needMoveFiles1.put(transactionDir + "/c3=4/c4=bbb/test2.txt", targetDir + "/c3=4/c4=bbb/test2.txt");
        Map<String, List<String>> partitionDirAndVals1 = new HashMap<>();
        partitionDirAndVals.put("/c3=4/c4=rrr", Arrays.stream((new String[]{"4", "rrr"})).collect(Collectors.toList()));
        partitionDirAndVals.put("/c3=4/c4=bbb", Arrays.stream((new String[]{"4", "bbb"})).collect(Collectors.toList()));
        FileCommitInfo fileCommitInfo1 = new FileCommitInfo(needMoveFiles1, partitionDirAndVals1, transactionDir);
        List<FileCommitInfo> fileCommitInfoList = new ArrayList<>();
        fileCommitInfoList.add(fileCommitInfo);
        fileCommitInfoList.add(fileCommitInfo1);

        FileAggregatedCommitInfo combine = fileSinkAggregatedCommitter.combine(fileCommitInfoList);
        Assert.assertEquals(1, combine.getTransactionMap().size());
        Assert.assertEquals(4, combine.getTransactionMap().get(transactionDir).size());
        Assert.assertEquals(targetDir + "/c3=3/c4=rrr/test1.txt", combine.getTransactionMap().get(transactionDir).get(transactionDir + "/c3=3/c4=rrr/test1.txt"));
        Assert.assertEquals(targetDir + "/c3=4/c4=bbb/test1.txt", combine.getTransactionMap().get(transactionDir).get(transactionDir + "/c3=4/c4=bbb/test1.txt"));
        Assert.assertEquals(targetDir + "/c3=4/c4=rrr/test2.txt", combine.getTransactionMap().get(transactionDir).get(transactionDir + "/c3=4/c4=rrr/test2.txt"));
        Assert.assertEquals(targetDir + "/c3=4/c4=bbb/test2.txt", combine.getTransactionMap().get(transactionDir).get(transactionDir + "/c3=4/c4=bbb/test2.txt"));
        Assert.assertEquals(3, combine.getPartitionDirAndValsMap().keySet().size());
    }

    @SuppressWarnings("checkstyle:UnnecessaryParentheses")
    @Test
    public void testAbort() throws Exception {
        FileSinkAggregatedCommitter fileSinkAggregatedCommitter = new FileSinkAggregatedCommitter(new HdfsFileSystemCommitter());
        Map<String, Map<String, String>> transactionFiles = new HashMap<>();
        Random random = new Random();
        Long jobId = random.nextLong();
        String transactionDir = String.format("/tmp/seatunnel/seatunnel/%s/T_%s_0_1", jobId, jobId);
        String targetDir = String.format("/tmp/hive/warehouse/%s", jobId);
        Map<String, String> needMoveFiles = new HashMap<>();
        needMoveFiles.put(transactionDir + "/c3=4/c4=rrr/test1.txt", targetDir + "/c3=4/c4=rrr/test1.txt");
        needMoveFiles.put(transactionDir + "/c3=4/c4=bbb/test1.txt", targetDir + "/c3=4/c4=bbb/test1.txt");
        Map<String, List<String>> partitionDirAndVals = new HashMap<>();
        partitionDirAndVals.put("/c3=4/c4=rrr", Arrays.stream((new String[]{"4", "rrr"})).collect(Collectors.toList()));
        partitionDirAndVals.put("/c3=4/c4=bbb", Arrays.stream((new String[]{"4", "bbb"})).collect(Collectors.toList()));
        HdfsUtils.createFile(transactionDir + "/c3=4/c4=rrr/test1.txt");
        HdfsUtils.createFile(transactionDir + "/c3=4/c4=bbb/test1.txt");

        transactionFiles.put(transactionDir, needMoveFiles);
        FileAggregatedCommitInfo fileAggregatedCommitInfo = new FileAggregatedCommitInfo(transactionFiles, partitionDirAndVals);
        List<FileAggregatedCommitInfo> fileAggregatedCommitInfoList = new ArrayList<>();
        fileAggregatedCommitInfoList.add(fileAggregatedCommitInfo);
        fileSinkAggregatedCommitter.commit(fileAggregatedCommitInfoList);

        Assert.assertTrue(HdfsUtils.fileExist(targetDir + "/c3=4/c4=bbb/test1.txt"));
        Assert.assertTrue(HdfsUtils.fileExist(targetDir + "/c3=4/c4=rrr/test1.txt"));
        Assert.assertTrue(!HdfsUtils.fileExist(transactionDir));

        fileSinkAggregatedCommitter.abort(fileAggregatedCommitInfoList);
        Assert.assertTrue(!HdfsUtils.fileExist(targetDir + "/c3=4/c4=bbb/test1.txt"));
        Assert.assertTrue(!HdfsUtils.fileExist(targetDir + "/c3=4/c4=rrr/test1.txt"));

        // transactionDir will being delete when abort
        Assert.assertTrue(!HdfsUtils.fileExist(transactionDir));
    }
}
