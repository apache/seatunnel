/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigList;
import io.github.interestinglab.waterdrop.config.ConfigMergeable;
import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigRenderOptions;
import io.github.interestinglab.waterdrop.config.ConfigValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

// This is just like ConfigDelayedMerge except we know statically
// that it will turn out to be an object.
final class ConfigDelayedMergeObject extends AbstractConfigObject implements Unmergeable,
        ReplaceableMergeStack {

    final private List<AbstractConfigValue> stack;

    ConfigDelayedMergeObject(ConfigOrigin origin, List<AbstractConfigValue> stack) {
        super(origin);
        this.stack = stack;

        if (stack.isEmpty())
            throw new ConfigException.BugOrBroken(
                    "creating empty delayed merge object");
        if (!(stack.get(0) instanceof AbstractConfigObject))
            throw new ConfigException.BugOrBroken(
                    "created a delayed merge object not guaranteed to be an object");

        for (AbstractConfigValue v : stack) {
            if (v instanceof ConfigDelayedMerge || v instanceof ConfigDelayedMergeObject)
                throw new ConfigException.BugOrBroken(
                        "placed nested DelayedMerge in a ConfigDelayedMergeObject, should have consolidated stack");
        }
    }

    @Override
    protected ConfigDelayedMergeObject newCopy(ResolveStatus status, ConfigOrigin origin) {
        if (status != resolveStatus())
            throw new ConfigException.BugOrBroken(
                    "attempt to create resolved ConfigDelayedMergeObject");
        return new ConfigDelayedMergeObject(origin, stack);
    }

    @Override
    ResolveResult<? extends AbstractConfigObject> resolveSubstitutions(ResolveContext context, ResolveSource source)
            throws NotPossibleToResolve {
        ResolveResult<? extends AbstractConfigValue> merged = ConfigDelayedMerge.resolveSubstitutions(this, stack,
                context, source);
        return merged.asObjectResult();
    }

    @Override
    public AbstractConfigValue makeReplacement(ResolveContext context, int skipping) {
        return ConfigDelayedMerge.makeReplacement(context, stack, skipping);
    }

    @Override
    ResolveStatus resolveStatus() {
        return ResolveStatus.UNRESOLVED;
    }

    @Override
    public AbstractConfigValue replaceChild(AbstractConfigValue child, AbstractConfigValue replacement) {
        List<AbstractConfigValue> newStack = replaceChildInList(stack, child, replacement);
        if (newStack == null)
            return null;
        else
            return new ConfigDelayedMergeObject(origin(), newStack);
    }

    @Override
    public boolean hasDescendant(AbstractConfigValue descendant) {
        return hasDescendantInList(stack, descendant);
    }

    @Override
    ConfigDelayedMergeObject relativized(Path prefix) {
        List<AbstractConfigValue> newStack = new ArrayList<AbstractConfigValue>();
        for (AbstractConfigValue o : stack) {
            newStack.add(o.relativized(prefix));
        }
        return new ConfigDelayedMergeObject(origin(), newStack);
    }

    @Override
    protected boolean ignoresFallbacks() {
        return ConfigDelayedMerge.stackIgnoresFallbacks(stack);
    }

    @Override
    protected final ConfigDelayedMergeObject mergedWithTheUnmergeable(Unmergeable fallback) {
        requireNotIgnoringFallbacks();

        return (ConfigDelayedMergeObject) mergedWithTheUnmergeable(stack, fallback);
    }

    @Override
    protected final ConfigDelayedMergeObject mergedWithObject(AbstractConfigObject fallback) {
        return mergedWithNonObject(fallback);
    }

    @Override
    protected final ConfigDelayedMergeObject mergedWithNonObject(AbstractConfigValue fallback) {
        requireNotIgnoringFallbacks();

        return (ConfigDelayedMergeObject) mergedWithNonObject(stack, fallback);
    }

    @Override
    public ConfigDelayedMergeObject withFallback(ConfigMergeable mergeable) {
        return (ConfigDelayedMergeObject) super.withFallback(mergeable);
    }

    @Override
    public ConfigDelayedMergeObject withOnlyKey(String key) {
        throw notResolved();
    }

    @Override
    public ConfigDelayedMergeObject withoutKey(String key) {
        throw notResolved();
    }

    @Override
    protected AbstractConfigObject withOnlyPathOrNull(Path path) {
        throw notResolved();
    }

    @Override
    AbstractConfigObject withOnlyPath(Path path) {
        throw notResolved();
    }

    @Override
    AbstractConfigObject withoutPath(Path path) {
        throw notResolved();
    }

    @Override
    public ConfigDelayedMergeObject withValue(String key, ConfigValue value) {
        throw notResolved();
    }

    @Override
    ConfigDelayedMergeObject withValue(Path path, ConfigValue value) {
        throw notResolved();
    }

    @Override
    public Collection<AbstractConfigValue> unmergedValues() {
        return stack;
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof ConfigDelayedMergeObject;
    }

    @Override
    public boolean equals(Object other) {
        // note that "origin" is deliberately NOT part of equality
        if (other instanceof ConfigDelayedMergeObject) {
            return canEqual(other)
                    && (this.stack == ((ConfigDelayedMergeObject) other).stack || this.stack
                            .equals(((ConfigDelayedMergeObject) other).stack));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // note that "origin" is deliberately NOT part of equality
        return stack.hashCode();
    }

    @Override
    protected void render(StringBuilder sb, int indent, boolean atRoot, String atKey, ConfigRenderOptions options) {
        ConfigDelayedMerge.render(stack, sb, indent, atRoot, atKey, options);
    }

    @Override
    protected void render(StringBuilder sb, int indent, boolean atRoot, ConfigRenderOptions options) {
        render(sb, indent, atRoot, null, options);
    }

    private static ConfigException notResolved() {
        return new ConfigException.NotResolved(
                "need to Config#resolve() before using this object, see the API docs for Config#resolve()");
    }

    @Override
    public Map<String, Object> unwrapped() {
        throw notResolved();
    }

    @Override
    public AbstractConfigValue get(Object key) {
        throw notResolved();
    }

    @Override
    public boolean containsKey(Object key) {
        throw notResolved();
    }

    @Override
    public boolean containsValue(Object value) {
        throw notResolved();
    }

    @Override
    public Set<Map.Entry<String, ConfigValue>> entrySet() {
        throw notResolved();
    }

    @Override
    public boolean isEmpty() {
        throw notResolved();
    }

    @Override
    public Set<String> keySet() {
        throw notResolved();
    }

    @Override
    public int size() {
        throw notResolved();
    }

    @Override
    public Collection<ConfigValue> values() {
        throw notResolved();
    }

    @Override
    protected AbstractConfigValue attemptPeekWithPartialResolve(String key) {
        // a partial resolve of a ConfigDelayedMergeObject always results in a
        // SimpleConfigObject because all the substitutions in the stack get
        // resolved in order to look up the partial.
        // So we know here that we have not been resolved at all even
        // partially.
        // Given that, all this code is probably gratuitous, since the app code
        // is likely broken. But in general we only throw NotResolved if you try
        // to touch the exact key that isn't resolved, so this is in that
        // spirit.

        // we'll be able to return a key if we have a value that ignores
        // fallbacks, prior to any unmergeable values.
        for (AbstractConfigValue layer : stack) {
            if (layer instanceof AbstractConfigObject) {
                AbstractConfigObject objectLayer = (AbstractConfigObject) layer;
                AbstractConfigValue v = objectLayer.attemptPeekWithPartialResolve(key);
                if (v != null) {
                    if (v.ignoresFallbacks()) {
                        // we know we won't need to merge anything in to this
                        // value
                        return v;
                    } else {
                        // we can't return this value because we know there are
                        // unmergeable values later in the stack that may
                        // contain values that need to be merged with this
                        // value. we'll throw the exception when we get to those
                        // unmergeable values, so continue here.
                        continue;
                    }
                } else if (layer instanceof Unmergeable) {
                    // an unmergeable object (which would be another
                    // ConfigDelayedMergeObject) can't know that a key is
                    // missing, so it can't return null; it can only return a
                    // value or throw NotPossibleToResolve
                    throw new ConfigException.BugOrBroken(
                            "should not be reached: unmergeable object returned null value");
                } else {
                    // a non-unmergeable AbstractConfigObject that returned null
                    // for the key in question is not relevant, we can keep
                    // looking for a value.
                    continue;
                }
            } else if (layer instanceof Unmergeable) {
                throw new ConfigException.NotResolved("Key '" + key + "' is not available at '"
                        + origin().description() + "' because value at '"
                        + layer.origin().description()
                        + "' has not been resolved and may turn out to contain or hide '" + key
                        + "'."
                        + " Be sure to Config#resolve() before using a config object.");
            } else if (layer.resolveStatus() == ResolveStatus.UNRESOLVED) {
                // if the layer is not an object, and not a substitution or
                // merge,
                // then it's something that's unresolved because it _contains_
                // an unresolved object... i.e. it's an array
                if (!(layer instanceof ConfigList))
                    throw new ConfigException.BugOrBroken("Expecting a list here, not " + layer);
                // all later objects will be hidden so we can say we won't find
                // the key
                return null;
            } else {
                // non-object, but resolved, like an integer or something.
                // has no children so the one we're after won't be in it.
                // we would only have this in the stack in case something
                // else "looks back" to it due to a cycle.
                // anyway at this point we know we can't find the key anymore.
                if (!layer.ignoresFallbacks()) {
                    throw new ConfigException.BugOrBroken(
                            "resolved non-object should ignore fallbacks");
                }
                return null;
            }
        }
        // If we get here, then we never found anything unresolved which means
        // the ConfigDelayedMergeObject should not have existed. some
        // invariant was violated.
        throw new ConfigException.BugOrBroken(
                "Delayed merge stack does not contain any unmergeable values");

    }
}
