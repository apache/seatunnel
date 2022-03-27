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

package org.apache.seatunnel.common.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public final class CheckConfigUtil {

    private CheckConfigUtil() {
    }

    /**
     * please using {@link #checkAllExists} instead, since 2.0.5
     */
    @Deprecated
    public static CheckResult check(Config config, String... params) {
        return checkAllExists(config, params);
    }

    public static CheckResult checkAllExists(Config config, String... params) {
        List<String> missingParams = Arrays.stream(params)
                .filter(param -> !isValidParam(config, param))
                .collect(Collectors.toList());

        if (!missingParams.isEmpty()) {
            String errorMsg = String.format("please specify [%s] as non-empty",
                    String.join(",", missingParams));
            return CheckResult.error(errorMsg);
        } else {
            return CheckResult.success();
        }
    }

    /**
     * check config if there was at least one usable
     */
    public static CheckResult checkAtLeastOneExists(Config config, String... params) {
        if (params.length == 0) {
            return CheckResult.success();
        }

        List<String> missingParams = new LinkedList<>();
        for (String param : params) {
            if (!isValidParam(config, param)) {
                missingParams.add(param);
            }
        }

        if (missingParams.size() == params.length) {
            String errorMsg = String.format("please specify at least one config of [%s] as non-empty",
                    String.join(",", missingParams));
            return CheckResult.error(errorMsg);
        } else {
            return CheckResult.success();
        }
    }

    public static boolean isValidParam(Config config, String param) {
        boolean isValidParam = true;
        if (!config.hasPath(param)) {
            isValidParam = false;
        } else if (config.getAnyRef(param) instanceof List) {
            isValidParam = !((List<?>) config.getAnyRef(param)).isEmpty();
        }
        return isValidParam;
    }

    /**
     * merge all check result
     */
    public static CheckResult mergeCheckResults(CheckResult... checkResults) {
        List<CheckResult> notPassConfig = Arrays.stream(checkResults)
                .filter(item -> !item.isSuccess()).collect(Collectors.toList());
        if (notPassConfig.isEmpty()) {
            return CheckResult.success();
        } else {
            String errMessage = notPassConfig.stream().map(CheckResult::getMsg)
                    .collect(Collectors.joining(","));
            return CheckResult.error(errMessage);
        }

    }
}
