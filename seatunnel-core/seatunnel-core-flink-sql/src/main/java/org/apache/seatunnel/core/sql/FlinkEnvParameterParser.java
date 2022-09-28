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

package org.apache.seatunnel.core.sql;

import org.apache.seatunnel.core.base.utils.CommandLineUtils;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.config.FlinkJobType;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public class FlinkEnvParameterParser {

    @SuppressWarnings("checkstyle:RegexpSingleline")
    public static void main(String[] args) {
        FlinkCommandArgs flinkCommandArgs = CommandLineUtils.parse(args, new FlinkCommandArgs(), FlinkJobType.SQL.getType(), true);
        List<String> envParameters = getEnvParameters(flinkCommandArgs);
        System.out.println(String.join(" ", envParameters));
    }

    static List<String> getEnvParameters(FlinkCommandArgs flinkCommandArgs) {
        return flinkCommandArgs.getVariables().stream()
            .filter(StringUtils::isNotBlank)
            .map(String::trim)
            .map(parameter -> "-D" + parameter)
            .collect(Collectors.toList());
    }
}
