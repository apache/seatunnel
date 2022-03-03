/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.sql.job;

import java.util.List;

public class JobInfo {

    private static final String JOB_NAME = "default sql job";
    private static final String VAR_REGEX = "\\$\\{%s}";
    private static final String DELIMITER = "=";

    private final String jobName;
    private String jobContent;

    public JobInfo(String jobContent) {
        this.jobName = JOB_NAME;
        this.jobContent = jobContent;
    }

    public JobInfo(String jobName, String jobContent) {
        this.jobName = jobName;
        this.jobContent = jobContent;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobContent() {
        return jobContent;
    }

    public void substitute(List<String> variables) {
        if (variables == null) {
            return;
        }
        for (String variable : variables) {
            String[] s = variable.split(DELIMITER);
            if (s.length == 2) {
                jobContent = jobContent.replaceAll(String.format(VAR_REGEX, s[0].trim()), s[1].trim());
            }
        }
    }

}
