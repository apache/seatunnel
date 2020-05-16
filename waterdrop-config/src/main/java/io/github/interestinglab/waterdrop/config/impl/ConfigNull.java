/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigRenderOptions;
import io.github.interestinglab.waterdrop.config.ConfigValueType;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * This exists because sometimes null is not the same as missing. Specifically,
 * if a value is set to null we can give a better error message (indicating
 * where it was set to null) in case someone asks for the value. Also, null
 * overrides values set "earlier" in the search path, while missing values do
 * not.
 *
 */
final class ConfigNull extends AbstractConfigValue implements Serializable {

    private static final long serialVersionUID = 2L;

    ConfigNull(ConfigOrigin origin) {
        super(origin);
    }

    @Override
    public ConfigValueType valueType() {
        return ConfigValueType.NULL;
    }

    @Override
    public Object unwrapped() {
        return null;
    }

    @Override
    String transformToString() {
        return "null";
    }

    @Override
    protected void render(StringBuilder sb, int indent, boolean atRoot, ConfigRenderOptions options) {
        sb.append("null");
    }

    @Override
    protected ConfigNull newCopy(ConfigOrigin origin) {
        return new ConfigNull(origin);
    }

    // serialization all goes through SerializedConfigValue
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedConfigValue(this);
    }
}
