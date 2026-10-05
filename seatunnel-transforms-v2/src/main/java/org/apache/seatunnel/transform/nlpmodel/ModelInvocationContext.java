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

package org.apache.seatunnel.transform.nlpmodel;

public class ModelInvocationContext {

    private final String provider;
    private final String model;
    private final int inputCount;
    private final int attempt;
    private final int requestTimeoutMs;

    public ModelInvocationContext(
            String provider, String model, int inputCount, int attempt, int requestTimeoutMs) {
        this.provider = provider;
        this.model = model;
        this.inputCount = inputCount;
        this.attempt = attempt;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public String getProvider() {
        return provider;
    }

    public String getModel() {
        return model;
    }

    public int getInputCount() {
        return inputCount;
    }

    public int getAttempt() {
        return attempt;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }
}
