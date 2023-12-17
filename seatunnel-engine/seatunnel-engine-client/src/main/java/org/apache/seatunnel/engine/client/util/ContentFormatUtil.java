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

package org.apache.seatunnel.engine.client.util;

import org.apache.seatunnel.engine.core.job.JobStatusData;

import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.List;

public class ContentFormatUtil {

    public static String format(List<JobStatusData> jobStatusDataList) {
        int maxJobIdLength = 6;
        int maxJobNameLength = 8;
        int maxJobStatusLength = 10;
        int maxSubmitTimeLength = 23;
        int maxFinishTimeLength = 23;

        for (JobStatusData jobStatusData : jobStatusDataList) {
            maxJobIdLength =
                    Math.max(maxJobIdLength, String.valueOf(jobStatusData.getJobId()).length());
            maxJobNameLength =
                    Math.max(maxJobNameLength, String.valueOf(jobStatusData.getJobName()).length());
            maxJobStatusLength =
                    Math.max(
                            maxJobStatusLength,
                            String.valueOf(jobStatusData.getJobStatus()).length());
        }

        String formatStr =
                "%-"
                        + (maxJobIdLength + 2)
                        + "s%-"
                        + (maxJobNameLength + 2)
                        + "s%-"
                        + (maxJobStatusLength + 2)
                        + "s%-"
                        + (maxSubmitTimeLength + 2)
                        + "s%-"
                        + (maxFinishTimeLength + 2)
                        + "s";
        String header =
                String.format(
                        formatStr,
                        "Job ID",
                        "Job Name",
                        "Job Status",
                        "Submit Time",
                        "Finished Time");
        String separator =
                String.format(
                        formatStr,
                        StringUtils.repeat("-", maxJobIdLength),
                        StringUtils.repeat("-", maxJobNameLength),
                        StringUtils.repeat("-", maxJobStatusLength),
                        StringUtils.repeat("-", maxSubmitTimeLength),
                        StringUtils.repeat("-", maxFinishTimeLength));

        StringBuilder sb = new StringBuilder();
        for (JobStatusData jobStatusData : jobStatusDataList) {
            String jobId = String.format("%-" + maxJobIdLength + "s", jobStatusData.getJobId());
            String jobName =
                    String.format("%-" + maxJobNameLength + "s", jobStatusData.getJobName());
            String jobStatus =
                    String.format("%-" + maxJobStatusLength + "s", jobStatusData.getJobStatus());
            String submitTime =
                    String.format(
                            "%-" + maxSubmitTimeLength + "s",
                            new Timestamp(jobStatusData.getSubmitTime()));
            String finishTime = "";
            if (jobStatusData.getFinishTime() != null) {
                finishTime =
                        String.format(
                                "%-" + maxFinishTimeLength + "s",
                                new Timestamp(jobStatusData.getFinishTime()));
            }
            sb.append(jobId)
                    .append("  ")
                    .append(jobName)
                    .append("  ")
                    .append(jobStatus)
                    .append("  ")
                    .append(submitTime)
                    .append("  ")
                    .append(finishTime)
                    .append("\n");
        }

        return header + "\n" + separator + "\n" + sb;
    }
}
