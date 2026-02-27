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

import lombok.Getter;

import java.io.Serializable;

@Getter
public class FormatterConfig<T extends Formatter> implements Serializable {
    private static final long serialVersionUID = 1L;
    private final T formatter;
    private final boolean userConfigured;

    private FormatterConfig(T formatter, boolean userConfigured) {
        this.formatter = formatter;
        this.userConfigured = userConfigured;
    }

    public static <T extends Formatter> FormatterConfig<T> ofDefault(T formatter) {
        return new FormatterConfig<>(formatter, false);
    }

    public static <T extends Formatter> FormatterConfig<T> ofUserConfigured(T formatter) {
        return new FormatterConfig<>(formatter, true);
    }
}
