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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.util;

public final class EdgeSocketLogUtils {

    private static final int DEFAULT_MAX_LOG_LENGTH = 80;

    private EdgeSocketLogUtils() {}

    public static String abbreviateForLog(String value) {
        return abbreviateForLog(value, DEFAULT_MAX_LOG_LENGTH);
    }

    public static String abbreviateForLog(String value, int maxLength) {
        if (value == null) {
            return "(null)";
        }
        if (maxLength <= 0) {
            return "...";
        }
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "...";
    }
}
