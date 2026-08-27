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

package org.apache.seatunnel.common.utils;

import lombok.NonNull;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtils {
    private ExceptionUtils() {}

    public static String getMessage(Throwable e) {
        if (e == null) {
            return "";
        }
        try (StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw)) {
            // Output the error stack information to the printWriter
            e.printStackTrace(pw);
            pw.flush();
            sw.flush();
            return sw.toString();
        } catch (Exception e1) {
            throw new RuntimeException("Failed to print exception logs", e1);
        }
    }

    public static Throwable getRootException(@NonNull Throwable e) {
        Throwable cause = e.getCause();
        if (cause != null) {
            return getRootException(cause);
        } else {
            return e;
        }
    }
}
