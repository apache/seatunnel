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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.client.util.ContentFormatUtil;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobStatusData;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class ContentFormatUtilTest {
    public static String getResource(String confFile) {
        return System.getProperty("user.dir") + "/src/test/resources/" + confFile;
    }

    public static String getClusterName(String testClassName) {
        return System.getProperty("user.name") + "_" + testClassName;
    }

    @Test
    public void testContentFormatUtil() throws InterruptedException {
        List<JobStatusData> statusDataList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            statusDataList.add(
                    new JobStatusData(
                            4352352414135L + i,
                            "Testfdsafew" + i,
                            JobStatus.CANCELING,
                            System.currentTimeMillis(),
                            System.currentTimeMillis(),
                            System.currentTimeMillis()));
            Thread.sleep(2L);
        }
        for (int i = 0; i < 5; i++) {
            statusDataList.add(
                    new JobStatusData(
                            4352352414135L + i,
                            "fdsafsddfasfsdafasdf" + i,
                            JobStatus.UNKNOWABLE,
                            System.currentTimeMillis(),
                            System.currentTimeMillis(),
                            null));
            Thread.sleep(2L);
        }

        statusDataList.sort(
                (s1, s2) -> {
                    if (s1.getSubmitTime() == s2.getSubmitTime()) {
                        return 0;
                    }
                    return s1.getSubmitTime() > s2.getSubmitTime() ? -1 : 1;
                });
        String r = ContentFormatUtil.format(statusDataList);
        log.info("\n" + r);
        List<JobStatusData> jobStatusDataList = parseTable(r);
        Assertions.assertEquals(10, jobStatusDataList.size());
        for (int i = 0; i < jobStatusDataList.size(); i++) {
            JobStatusData jobStatusData = jobStatusDataList.get(i);
            JobStatusData statusData = statusDataList.get(i);
            Assertions.assertEquals(statusData.getJobId(), jobStatusData.getJobId());
            Assertions.assertEquals(statusData.getJobName(), jobStatusData.getJobName());
            Assertions.assertEquals(statusData.getJobStatus(), jobStatusData.getJobStatus());
            Assertions.assertEquals(statusData.getSubmitTime(), jobStatusData.getSubmitTime());
            Assertions.assertEquals(statusData.getStartTime(), jobStatusData.getStartTime());
            Assertions.assertEquals(statusData.getFinishTime(), jobStatusData.getFinishTime());
        }
    }

    private List<JobStatusData> parseTable(String tableData) {
        List<JobStatusData> result = new ArrayList<>();
        String[] lines = tableData.split("\n");

        int startIndex = 2;
        if (lines.length <= startIndex) {
            return result;
        }

        Pattern pattern =
                Pattern.compile(
                        // Job ID
                        "^\\s*(\\d+)\\s+"
                                + // Job Name
                                "(.+?)\\s+"
                                + // Job Status
                                "(UNKNOWABLE|CANCELING|CANCELED|RUNNING|FINISHED|FAILED)\\s+"
                                + // Submit Time
                                "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+)\\s+"
                                + // Start Time
                                "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+)\\s*"
                                + // Finished Time
                                "(.*?)$");

        for (int i = startIndex; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) {
                continue;
            }

            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                JobStatusData jobStatusData = new JobStatusData();
                jobStatusData.setJobId(Long.parseLong(matcher.group(1)));
                jobStatusData.setJobName(matcher.group(2));
                jobStatusData.setJobStatus(JobStatus.valueOf(matcher.group(3)));
                jobStatusData.setSubmitTime(Timestamp.valueOf(matcher.group(4)).getTime());
                jobStatusData.setStartTime(Timestamp.valueOf(matcher.group(5)).getTime());
                jobStatusData.setFinishTime(
                        matcher.group(6).isEmpty()
                                ? null
                                : Timestamp.valueOf(matcher.group(6)).getTime());
                result.add(jobStatusData);
            }
        }
        return result;
    }
}
