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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Strings;
import com.beust.jcommander.UnixStyleUsageFormatter;
import com.beust.jcommander.WrappedParameter;

import java.util.List;

public class UsageFormatter extends UnixStyleUsageFormatter {
    private static final int INDENT = 3;
    public UsageFormatter(JCommander commander) {
        super(commander);
    }

    @Override
    public void appendAllParametersDetails(StringBuilder out,
                                           int indentCount,
                                           String indent,
                                           List<ParameterDescription> sortedParameters) {
        if (sortedParameters.size() > 0) {
            out.append(indent).append("  Options:\n");
        }

        // Calculate prefix indent
        int prefixIndent = 0;

        for (ParameterDescription pd : sortedParameters) {
            WrappedParameter parameter = pd.getParameter();
            String prefix = (parameter.required() ? "* " : "  ") + pd.getNames();

            if (prefix.length() > prefixIndent) {
                prefixIndent = prefix.length();
            }
        }

        // Append parameters
        for (ParameterDescription pd : sortedParameters) {
            WrappedParameter parameter = pd.getParameter();

            String prefix = (parameter.required() ? "* " : "  ") + pd.getNames();
            out.append(indent)
                    .append("  ")
                    .append(prefix)
                    .append(s(prefixIndent - prefix.length()))
                    .append(" ");
            final int initialLinePrefixLength = indent.length() + prefixIndent + 3;

            // Generate description
            String description = pd.getDescription();
            Object def = pd.getDefault();

            if (pd.isDynamicParameter()) {
                String syntax = "(syntax: " + parameter.names()[0] + "key" + parameter.getAssignment() + "value)";
                description += (description.length() == 0 ? "" : " ") + syntax;
            }
            Class<?> type = pd.getParameterized().getType();
            if (def != null && !pd.isHelp()) {
                String displayText = type.isEnum() ? def.toString().toLowerCase() : def.toString();
                String displayedDef = Strings.isStringEmpty(displayText) ? "<empty string>" : displayText;
                String defaultText = "(default: " + (parameter.password() ? "********" : displayedDef) + ")";
                description += (description.length() == 0 ? "" : " ") + defaultText;
            }
            wrapDescription(out, indentCount + prefixIndent - INDENT, initialLinePrefixLength, description);
            out.append("\n");
        }
    }
}

