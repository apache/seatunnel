package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigSyntax;

import java.util.ArrayList;
import java.util.Collection;

final class ConfigNodeRoot extends ConfigNodeComplexValue {
    final private ConfigOrigin origin;

    ConfigNodeRoot(Collection<AbstractConfigNode> children, ConfigOrigin origin) {
        super(children);
        this.origin = origin;
    }

    @Override
    protected ConfigNodeRoot newNode(Collection<AbstractConfigNode> nodes) {
        throw new ConfigException.BugOrBroken("Tried to indent the root object");
    }

    protected ConfigNodeComplexValue value() {
        for (AbstractConfigNode node : children) {
            if (node instanceof ConfigNodeComplexValue) {
                return (ConfigNodeComplexValue)node;
            }
        }
        throw new ConfigException.BugOrBroken("ConfigNodeRoot did not contain a value");
    }

    protected ConfigNodeRoot setValue(String desiredPath, AbstractConfigNodeValue value, ConfigSyntax flavor) {
        ArrayList<AbstractConfigNode> childrenCopy = new ArrayList<AbstractConfigNode>(children);
        for (int i = 0; i < childrenCopy.size(); i++) {
            AbstractConfigNode node = childrenCopy.get(i);
            if (node instanceof ConfigNodeComplexValue) {
                if (node instanceof ConfigNodeArray) {
                    throw new ConfigException.WrongType(origin, "The ConfigDocument had an array at the root level, and values cannot be modified inside an array.");
                } else if (node instanceof ConfigNodeObject) {
                    if (value == null) {
                        childrenCopy.set(i, ((ConfigNodeObject)node).removeValueOnPath(desiredPath, flavor));
                    } else {
                        childrenCopy.set(i, ((ConfigNodeObject) node).setValueOnPath(desiredPath, value, flavor));
                    }
                    return new ConfigNodeRoot(childrenCopy, origin);
                }
            }
        }
        throw new ConfigException.BugOrBroken("ConfigNodeRoot did not contain a value");
    }

    protected boolean hasValue(String desiredPath) {
        Path path = PathParser.parsePath(desiredPath);
        ArrayList<AbstractConfigNode> childrenCopy = new ArrayList<AbstractConfigNode>(children);
        for (int i = 0; i < childrenCopy.size(); i++) {
            AbstractConfigNode node = childrenCopy.get(i);
            if (node instanceof ConfigNodeComplexValue) {
                if (node instanceof ConfigNodeArray) {
                    throw new ConfigException.WrongType(origin, "The ConfigDocument had an array at the root level, and values cannot be modified inside an array.");
                } else if (node instanceof ConfigNodeObject) {
                    return ((ConfigNodeObject) node).hasValue(path);
                }
            }
        }
        throw new ConfigException.BugOrBroken("ConfigNodeRoot did not contain a value");
    }
}
