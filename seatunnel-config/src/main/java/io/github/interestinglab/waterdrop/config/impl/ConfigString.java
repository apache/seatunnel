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
import io.github.interestinglab.waterdrop.config.ConfigRenderOptions;
import io.github.interestinglab.waterdrop.config.ConfigValueType;

import java.io.ObjectStreamException;
import java.io.Serializable;

abstract class ConfigString extends AbstractConfigValue implements Serializable {

    private static final long serialVersionUID = 2L;

    protected final String value;

    protected ConfigString(ConfigOrigin origin, String value) {
        super(origin);
        this.value = value;
    }

    static final class Quoted extends ConfigString {
        Quoted(ConfigOrigin origin, String value) {
            super(origin, value);
        }

        @Override
        protected Quoted newCopy(ConfigOrigin origin) {
            return new Quoted(origin, value);
        }

        // serialization all goes through SerializedConfigValue
        private Object writeReplace() throws ObjectStreamException {
            return new SerializedConfigValue(this);
        }
    }

    // this is sort of a hack; we want to preserve whether whitespace
    // was quoted until we process substitutions, so we can ignore
    // unquoted whitespace when concatenating lists or objects.
    // We dump this distinction when serializing and deserializing,
    // but that's OK because it isn't in equals/hashCode, and we
    // don't allow serializing unresolved objects which is where
    // quoted-ness matters. If we later make ConfigOrigin point
    // to the original token range, we could use that to implement
    // wasQuoted()
    static final class Unquoted extends ConfigString {
        Unquoted(ConfigOrigin origin, String value) {
            super(origin, value);
        }

        @Override
        protected Unquoted newCopy(ConfigOrigin origin) {
            return new Unquoted(origin, value);
        }

        // serialization all goes through SerializedConfigValue
        private Object writeReplace() throws ObjectStreamException {
            return new SerializedConfigValue(this);
        }
    }

    boolean wasQuoted() {
        return this instanceof Quoted;
    }

    @Override
    public ConfigValueType valueType() {
        return ConfigValueType.STRING;
    }

    @Override
    public String unwrapped() {
        return value;
    }

    @Override
    String transformToString() {
        return value;
    }

    @Override
    protected void render(StringBuilder sb, int indent, boolean atRoot, ConfigRenderOptions options) {
        String rendered;
        if (options.getJson()) {
            rendered = ConfigImplUtil.renderJsonString(value);
        } else {
            rendered = ConfigImplUtil.renderStringUnquotedIfPossible(value);
        }
        sb.append(rendered);
    }
}
