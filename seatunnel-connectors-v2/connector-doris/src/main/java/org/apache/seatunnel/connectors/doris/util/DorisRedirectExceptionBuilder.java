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

package org.apache.seatunnel.connectors.doris.util;

public final class DorisRedirectExceptionBuilder {

    private DorisRedirectExceptionBuilder() {}

    public static String build(
            String requestUrl,
            String location,
            boolean directToBe,
            boolean enable2PC,
            String requestStage) {
        return String.format(
                "stream load redirect not followed: HTTP/1.1 307 Temporary Redirect, "
                        + "request=%s, Location=%s, direct_to_be=%s, 2pc=%s, stage=%s. "
                        + "Please check BE reachability, FE load, and consider benodes + direct_to_be=true when FE redirect is unstable.",
                requestUrl,
                location == null ? "<missing>" : location,
                directToBe,
                enable2PC,
                requestStage);
    }

    public static String buildFollowUpFailure(
            String requestUrl,
            String location,
            boolean directToBe,
            boolean enable2PC,
            String requestStage,
            String causeMessage) {
        return String.format(
                "stream load redirect follow-up failed after HTTP/1.1 307 Temporary Redirect, "
                        + "request=%s, Location=%s, direct_to_be=%s, 2pc=%s, stage=%s, cause=%s. "
                        + "Please check BE reachability, FE load, and consider benodes + direct_to_be=true when FE redirect is unstable.",
                requestUrl,
                location == null ? "<missing>" : location,
                directToBe,
                enable2PC,
                requestStage,
                causeMessage == null ? "<missing>" : causeMessage);
    }
}
