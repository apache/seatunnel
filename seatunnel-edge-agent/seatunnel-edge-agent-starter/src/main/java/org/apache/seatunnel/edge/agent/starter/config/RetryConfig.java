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

package org.apache.seatunnel.edge.agent.starter.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;

import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

@Getter
public class RetryConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int maxAttempts;
    private final long backoffMs;
    private final long backoffMaxMs;

    public RetryConfig(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config");
        ConfigValidator.of(config).validate(EdgeAgentRuntimeOptionRules.retryRule());

        this.maxAttempts = config.get(EdgeAgentRuntimeOptions.RETRY_MAX_ATTEMPTS);
        if (this.maxAttempts < 1) {
            throw new IllegalArgumentException("retry.max-attempts must be >= 1.");
        }
        this.backoffMs = config.get(EdgeAgentRuntimeOptions.RETRY_BACKOFF_MS);
        if (this.backoffMs < 0L) {
            throw new IllegalArgumentException("retry.backoff-ms must be >= 0.");
        }
        this.backoffMaxMs = config.get(EdgeAgentRuntimeOptions.RETRY_BACKOFF_MAX_MS);
        if (this.backoffMaxMs < this.backoffMs) {
            throw new IllegalArgumentException(
                    "retry.backoff-max-ms must be >= effective retry.backoff-ms ("
                            + this.backoffMs
                            + " ms).");
        }
    }

    public static RetryConfig from(ReadonlyConfig config) {
        return new RetryConfig(config);
    }
}
