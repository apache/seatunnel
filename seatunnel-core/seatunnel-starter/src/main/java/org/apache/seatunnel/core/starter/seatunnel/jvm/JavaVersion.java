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

package org.apache.seatunnel.core.starter.seatunnel.jvm;

import org.apache.seatunnel.common.exception.CommonErrorCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JavaVersion {

    public static final List<Integer> CURRENT =
            parse(System.getProperty("java.specification.version"));

    static List<Integer> parse(final String value) {
        if (!value.matches("^0*[0-9]+(\\.[0-9]+)*$")) {
            throw new JvmOptionsParserException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    String.format("JAVA version [%s] of the system is incorrect", value));
        }

        final List<Integer> version = new ArrayList<Integer>();
        final String[] components = value.split("\\.");
        for (final String component : components) {
            version.add(Integer.valueOf(component));
        }
        return version;
    }

    public static int majorVersion(final List<Integer> javaVersion) {
        Objects.requireNonNull(javaVersion);
        if (javaVersion.get(0) > 1) {
            return javaVersion.get(0);
        } else {
            return javaVersion.get(1);
        }
    }
}
