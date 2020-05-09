/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
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
        if (parseable == this.parseable)
            return this;
        else
            return new SimpleIncludeContext(parseable);
    }

    @Override
    public ConfigParseable relativeTo(String filename) {
        if (ConfigImpl.traceLoadsEnabled())
            ConfigImpl.trace("Looking for '" + filename + "' relative to " + parseable);
        if (parseable != null)
            return parseable.relativeTo(filename);
        else
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
