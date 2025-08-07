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

package org.apache.seatunnel.engine.server.event;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.engine.core.job.JobStatus;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@ToString
public class JobStateEvent implements Event {

    private String jobName;
    private String jobId;
    private JobStatus jobStatus;
    private long createdTime;

    public JobStateEvent(String fullJobName, JobStatus jobStatus) {
        Pattern compile = Pattern.compile("Job\\s+(\\S+)\\s+\\(([^)]+)\\)");
        Matcher matcher = compile.matcher(fullJobName);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid job name format: " + fullJobName);
        }
        this.jobName = matcher.group(1);
        this.jobId = matcher.group(2);
        this.jobStatus = jobStatus;
        this.createdTime = System.currentTimeMillis();
    }

    @Override
    public EventType getEventType() {
        return null;
    }
}
