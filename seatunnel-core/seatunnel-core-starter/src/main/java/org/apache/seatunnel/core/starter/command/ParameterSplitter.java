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
package org.apache.seatunnel.core.starter.command;

import com.beust.jcommander.converters.IParameterSplitter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParameterSplitter implements IParameterSplitter {

    Pattern pattern = Pattern.compile("\\[.*?]|,");

    @Override
    public List<String> split(String value) {
        if (!value.contains(",")) {
            return Collections.singletonList(value);
        }
        Matcher matcher = pattern.matcher(value);
        StringBuilder stringBuilder = new StringBuilder();
        int start = 0;
        while (matcher.find()) {
            stringBuilder.append(value, start, matcher.start());
            if (matcher.group().equals(",")) {
                stringBuilder.append(";");
            } else {
                stringBuilder.append(matcher.group());
            }
            start = matcher.end();
        }
        stringBuilder.append(value.substring(start)); 

        return Arrays.asList(stringBuilder.toString().split(";"));
    }
}
