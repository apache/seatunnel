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

import java.util.Collections;

public class StringFormatUtils {
    private static final int NUM = 47;

    private StringFormatUtils() {
        // utility class can not be instantiated
    }

    public static String formatTable(Object... objects) {
        String title = objects[0].toString();
        int blankNum = (NUM - title.length()) / 2;
        int kvNum = (objects.length - 1) / 2;
        String template =
                "\n"
                        + "***********************************************"
                        + "\n"
                        + String.join("", Collections.nCopies(blankNum, " "))
                        + "%s"
                        + "\n"
                        + "***********************************************"
                        + "\n"
                        + String.join("", Collections.nCopies(kvNum, "%-26s: %19s\n"))
                        + "***********************************************\n";
        return String.format(template, objects);
    }
}
