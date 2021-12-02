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

import io.github.interestinglab.waterdrop.config.ConfigIncludeContext;
import io.github.interestinglab.waterdrop.config.ConfigParseOptions;
import io.github.interestinglab.waterdrop.config.ConfigParseable;

class SimpleIncludeContext implements ConfigIncludeContext {

    private final Parseable parseable;
    private final ConfigParseOptions options;

    SimpleIncludeContext(Parseable parseable) {
        this.parseable = parseable;
        this.options = SimpleIncluder.clearForInclude(parseable.options());
    }

    private SimpleIncludeContext(Parseable parseable, ConfigParseOptions options) {
        this.parseable = parseable;
        this.options = options;
    }

    SimpleIncludeContext withParseable(Parseable parseable) {
        if (parseable == this.parseable) {
            return this;
        }
        return new SimpleIncludeContext(parseable);
    }

    @Override
    public ConfigParseable relativeTo(String filename) {
        if (ConfigImpl.traceLoadsEnable()) {
            ConfigImpl.trace("Looking for '" + filename + "' relative to " + parseable);
        }
        if (parseable != null) {
            return parseable.relativeTo(filename);
        }
        return null;
    }

    @Override
    public ConfigParseOptions parseOptions() {
        return options;
    }

    @Override
    public ConfigIncludeContext setParseOptions(ConfigParseOptions options) {
        return new SimpleIncludeContext(parseable, options.setSyntax(null).setOriginDescription(null));
    }
}
