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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {
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
        System.out.println(r);
    }
}
