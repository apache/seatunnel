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
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ParameterSplitter implements IParameterSplitter {

    @Override
    public List<String> split(String value) {

        List<String> result = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean insideBrackets = false;
        boolean insideQuotes = false;

        for (char c : value.toCharArray()) {

            if (c == '[') {
                insideBrackets = true;
            } else if (c == ']') {
                insideBrackets = false;
            } else if (c == '"') {
                insideQuotes = !insideQuotes;
            }

            if (c == ',' && !insideQuotes && !insideBrackets) {
                result.add(currentToken.toString().trim());
                currentToken = new StringBuilder();
            } else {
                currentToken.append(c);
            }
        }

        if (currentToken.length() > 0) {
            result.add(currentToken.toString().trim());
        }

        return result.stream()
                .map(
                        variable -> {
                            String key = variable.split("=")[0];
                            String func = variable.split("=")[1];
                            Pattern pattern = Pattern.compile("func\\('(.+?)'\\)");
                            Matcher matcher = pattern.matcher(func);

                            while (matcher.find()) {
                                String groovyFunction = matcher.group(1);
                                func =
                                        func.replace(
                                                matcher.group(0),
                                                executeGroovyFunction(groovyFunction).toString());
                            }
                            return key + "=" + func;
                        })
                .collect(Collectors.toList());
    }

    public static Object executeGroovyFunction(String groovyFunction) {
        Binding binding = new Binding();
        GroovyShell shell = new GroovyShell(binding);

        Script script = shell.parse(groovyFunction);
        return script.run();
    }
}
