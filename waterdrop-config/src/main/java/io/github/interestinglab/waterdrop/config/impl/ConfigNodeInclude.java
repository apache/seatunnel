package io.github.interestinglab.waterdrop.config.impl;

import java.util.ArrayList;
import java.util.Collection;

final class ConfigNodeInclude extends AbstractConfigNode {
    final private ArrayList<AbstractConfigNode> children;
    final private ConfigIncludeKind kind;
    final private boolean isRequired;

    ConfigNodeInclude(Collection<AbstractConfigNode> children, ConfigIncludeKind kind, boolean isRequired) {
        this.children = new ArrayList<AbstractConfigNode>(children);
        this.kind = kind;
        this.isRequired = isRequired;
    }

    final public Collection<AbstractConfigNode> children() {
        return children;
    }

    @Override
    protected Collection<Token> tokens() {
        ArrayList<Token> tokens = new ArrayList<Token>();
        for (AbstractConfigNode child : children) {
            tokens.addAll(child.tokens());
        }
        return tokens;
    }

    protected ConfigIncludeKind kind() {
        return kind;
    }

    protected boolean isRequired() {
        return isRequired;
    }

    protected String name() {
        for (AbstractConfigNode n : children) {
            if (n instanceof ConfigNodeSimpleValue) {
                return (String)Tokens.getValue(((ConfigNodeSimpleValue) n).token()).unwrapped();
            }
        }
        return null;
    }
}
