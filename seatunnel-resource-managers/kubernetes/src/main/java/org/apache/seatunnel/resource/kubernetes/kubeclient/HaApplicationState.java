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

package org.apache.seatunnel.resource.kubernetes.kubeclient;

import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/** Non-secret lifecycle metadata on the durable owner Job, protected by namespace RBAC. */
public final class HaApplicationState {
    public static final String ENABLED = "seatunnel.apache.org/ha";
    public static final String INTENT = "seatunnel.apache.org/submission";
    public static final String JOB_ID = "seatunnel.apache.org/job-id";
    public static final String SPEC_DIGEST = "seatunnel.apache.org/spec-digest";
    public static final String STATUS = "seatunnel.apache.org/application-status";

    private HaApplicationState() {}

    public static Map<String, String> annotations(KubernetesJob job) {
        Map<String, String> annotations = job.getInternalResource().getMetadata().getAnnotations();
        return annotations == null ? java.util.Collections.emptyMap() : annotations;
    }

    public static boolean enabled(KubernetesJob job) {
        return "true".equals(annotations(job).get(ENABLED));
    }

    public static ApplicationStatus terminal(KubernetesJob job) {
        if (!enabled(job)) {
            return null;
        }
        String value = annotations(job).get(STATUS);
        for (ApplicationStatus status :
                new ApplicationStatus[] {
                    ApplicationStatus.SUCCEEDED,
                    ApplicationStatus.FAILED,
                    ApplicationStatus.CANCELED
                }) {
            if (status.name().equals(value)) {
                return status;
            }
        }
        return null;
    }

    public static String digest(String specification) {
        try {
            byte[] bytes =
                    MessageDigest.getInstance("SHA-256")
                            .digest(specification.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder();
            for (byte b : bytes) {
                result.append(String.format("%02x", b & 255));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
