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

package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigValueType;

import java.io.ObjectStreamException;
import java.io.Serializable;

final class ConfigBoolean extends AbstractConfigValue implements Serializable {

    private static final long serialVersionUID = 2L;

    private final boolean value;

    ConfigBoolean(ConfigOrigin origin, boolean value) {
        super(origin);
        this.value = value;
    }

    @Override
    public ConfigValueType valueType() {
        return ConfigValueType.BOOLEAN;
    }

    @Override
    public Boolean unwrapped() {
        return value;
    }

    @Override
    String transformToString() {
        return value ? "true" : "false";
    }

    @Override
    protected ConfigBoolean newCopy(ConfigOrigin origin) {
        return new ConfigBoolean(origin, value);
    }

    // serialization all goes through SerializedConfigValue
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedConfigValue(this);
    }
}
