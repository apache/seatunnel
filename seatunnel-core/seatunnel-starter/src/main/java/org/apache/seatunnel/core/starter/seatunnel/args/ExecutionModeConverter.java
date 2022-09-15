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

package org.apache.seatunnel.core.starter.seatunnel.args;

import org.apache.seatunnel.engine.common.runtime.ExecutionMode;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public class ExecutionModeConverter implements IStringConverter<ExecutionMode> {
    @Override
    public ExecutionMode convert(String value) {
        try {
            return ExecutionMode.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ParameterException("execution-mode: " + value + " is not allowed.");
        }
    }
}
