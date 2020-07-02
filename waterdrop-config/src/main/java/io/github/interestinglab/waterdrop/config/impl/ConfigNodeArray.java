package io.github.interestinglab.waterdrop.config.impl;

import java.util.Collection;

final class ConfigNodeArray extends ConfigNodeComplexValue {
    ConfigNodeArray(Collection<AbstractConfigNode> children) {
        super(children);
    }

    @Override
    protected ConfigNodeArray newNode(Collection<AbstractConfigNode> nodes) {
        return new ConfigNodeArray(nodes);
    }
}
