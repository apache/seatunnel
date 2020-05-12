package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;

// value is allowed to be null
final class ResolveResult<V extends AbstractConfigValue> {
    public final ResolveContext context;
    public final V value;

    private ResolveResult(ResolveContext context, V value) {
        this.context = context;
        this.value = value;
    }

    static <V extends AbstractConfigValue> ResolveResult<V> make(ResolveContext context, V value) {
        return new ResolveResult<V>(context, value);
    }

    // better option? we don't have variance
    @SuppressWarnings("unchecked")
    ResolveResult<AbstractConfigObject> asObjectResult() {
        if (!(value instanceof AbstractConfigObject))
            throw new ConfigException.BugOrBroken("Expecting a resolve result to be an object, but it was " + value);
        Object o = this;
        return (ResolveResult<AbstractConfigObject>) o;
    }

    // better option? we don't have variance
    @SuppressWarnings("unchecked")
    ResolveResult<AbstractConfigValue> asValueResult() {
        Object o = this;
        return (ResolveResult<AbstractConfigValue>) o;
    }

    ResolveResult<V> popTrace() {
        return make(context.popTrace(), value);
    }

    @Override
    public String toString() {
        return "ResolveResult(" + value + ")";
    }
}
