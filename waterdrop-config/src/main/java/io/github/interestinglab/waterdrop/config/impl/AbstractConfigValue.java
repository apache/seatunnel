/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigMergeable;
import io.github.interestinglab.waterdrop.config.ConfigObject;
import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigRenderOptions;
import io.github.interestinglab.waterdrop.config.ConfigValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 * Trying very hard to avoid a parent reference in config values; when you have
 * a tree like this, the availability of parent() tends to result in a lot of
 * improperly-factored and non-modular code. Please don't add parent().
 *
 */
abstract class AbstractConfigValue implements ConfigValue, MergeableValue {

    final private SimpleConfigOrigin origin;

    AbstractConfigValue(ConfigOrigin origin) {
        this.origin = (SimpleConfigOrigin) origin;
    }

    @Override
    public SimpleConfigOrigin origin() {
        return this.origin;
    }

    /**
     * This exception means that a value is inherently not resolveable, at the
     * moment the only known cause is a cycle of substitutions. This is a
     * checked exception since it's internal to the library and we want to be
     * sure we handle it before passing it out to public API. This is only
     * supposed to be thrown by the target of a cyclic reference and it's
     * supposed to be caught by the ConfigReference looking up that reference,
     * so it should be impossible for an outermost resolve() to throw this.
     *
     * Contrast with ConfigException.NotResolved which just means nobody called
     * resolve().
     */
    static class NotPossibleToResolve extends Exception {
        private static final long serialVersionUID = 1L;

        final private String traceString;

        NotPossibleToResolve(ResolveContext context) {
            super("was not possible to resolve");
            this.traceString = context.traceString();
        }

        String traceString() {
            return traceString;
        }
    }

    /**
     * Called only by ResolveContext.resolve().
     *
     * @param context
     *            state of the current resolve
     * @param source
     *            where to look up values
     * @return a new value if there were changes, or this if no changes
     */
    ResolveResult<? extends AbstractConfigValue> resolveSubstitutions(ResolveContext context, ResolveSource source)
            throws NotPossibleToResolve {
        return ResolveResult.make(context, this);
    }

    ResolveStatus resolveStatus() {
        return ResolveStatus.RESOLVED;
    }

    protected static List<AbstractConfigValue> replaceChildInList(List<AbstractConfigValue> list,
                                                                  AbstractConfigValue child, AbstractConfigValue replacement) {
        int i = 0;
        while (i < list.size() && list.get(i) != child)
            ++i;
        if (i == list.size())
            throw new ConfigException.BugOrBroken("tried to replace " + child + " which is not in " + list);
        List<AbstractConfigValue> newStack = new ArrayList<AbstractConfigValue>(list);
        if (replacement != null)
            newStack.set(i, replacement);
        else
            newStack.remove(i);

        if (newStack.isEmpty())
            return null;
        else
            return newStack;
    }

    protected static boolean hasDescendantInList(List<AbstractConfigValue> list, AbstractConfigValue descendant) {
        for (AbstractConfigValue v : list) {
            if (v == descendant)
                return true;
        }
        // now the expensive traversal
        for (AbstractConfigValue v : list) {
            if (v instanceof Container && ((Container) v).hasDescendant(descendant))
                return true;
        }
        return false;
    }

    /**
     * This is used when including one file in another; the included file is
     * relativized to the path it's included into in the parent file. The point
     * is that if you include a file at foo.bar in the parent, and the included
     * file as a substitution ${a.b.c}, the included substitution now needs to
     * be ${foo.bar.a.b.c} because we resolve substitutions globally only after
     * parsing everything.
     *
     * @param prefix
     * @return value relativized to the given path or the same value if nothing
     *         to do
     */
    AbstractConfigValue relativized(Path prefix) {
        return this;
    }

    protected interface Modifier {
        // keyOrNull is null for non-objects
        AbstractConfigValue modifyChildMayThrow(String keyOrNull, AbstractConfigValue v)
                throws Exception;
    }

    protected abstract class NoExceptionsModifier implements Modifier {
        @Override
        public final AbstractConfigValue modifyChildMayThrow(String keyOrNull, AbstractConfigValue v)
                throws Exception {
            try {
                return modifyChild(keyOrNull, v);
            } catch (RuntimeException e) {
                throw e;
            } catch(Exception e) {
                throw new ConfigException.BugOrBroken("Unexpected exception", e);
            }
        }

        abstract AbstractConfigValue modifyChild(String keyOrNull, AbstractConfigValue v);
    }

    @Override
    public AbstractConfigValue toFallbackValue() {
        return this;
    }

    protected abstract AbstractConfigValue newCopy(ConfigOrigin origin);

    // this is virtualized rather than a field because only some subclasses
    // really need to store the boolean, and they may be able to pack it
    // with another boolean to save space.
    protected boolean ignoresFallbacks() {
        // if we are not resolved, then somewhere in this value there's
        // a substitution that may need to look at the fallbacks.
        return resolveStatus() == ResolveStatus.RESOLVED;
    }

    protected AbstractConfigValue withFallbacksIgnored() {
        if (ignoresFallbacks())
            return this;
        else
            throw new ConfigException.BugOrBroken(
                    "value class doesn't implement forced fallback-ignoring " + this);
    }

    // the withFallback() implementation is supposed to avoid calling
    // mergedWith* if we're ignoring fallbacks.
    protected final void requireNotIgnoringFallbacks() {
        if (ignoresFallbacks())
            throw new ConfigException.BugOrBroken(
                    "method should not have been called with ignoresFallbacks=true "
                            + getClass().getSimpleName());
    }

    protected AbstractConfigValue constructDelayedMerge(ConfigOrigin origin,
            List<AbstractConfigValue> stack) {
        return new ConfigDelayedMerge(origin, stack);
    }

    protected final AbstractConfigValue mergedWithTheUnmergeable(
      Collection<AbstractConfigValue> stack, Unmergeable fallback) {
        requireNotIgnoringFallbacks();

        // if we turn out to be an object, and the fallback also does,
        // then a merge may be required; delay until we resolve.
        List<AbstractConfigValue> newStack = new ArrayList<AbstractConfigValue>();
        newStack.addAll(stack);
        newStack.addAll(fallback.unmergedValues());
        return constructDelayedMerge(AbstractConfigObject.mergeOrigins(newStack), newStack);
    }

    private final AbstractConfigValue delayMerge(Collection<AbstractConfigValue> stack,
                                                 AbstractConfigValue fallback) {
        // if we turn out to be an object, and the fallback also does,
        // then a merge may be required.
        // if we contain a substitution, resolving it may need to look
        // back to the fallback.
        List<AbstractConfigValue> newStack = new ArrayList<AbstractConfigValue>();
        newStack.addAll(stack);
        newStack.add(fallback);
        return constructDelayedMerge(AbstractConfigObject.mergeOrigins(newStack), newStack);
    }

    protected final AbstractConfigValue mergedWithObject(Collection<AbstractConfigValue> stack,
                                                         AbstractConfigObject fallback) {
        requireNotIgnoringFallbacks();

        if (this instanceof AbstractConfigObject)
            throw new ConfigException.BugOrBroken("Objects must reimplement mergedWithObject");

        return mergedWithNonObject(stack, fallback);
    }

    protected final AbstractConfigValue mergedWithNonObject(Collection<AbstractConfigValue> stack,
                                                            AbstractConfigValue fallback) {
        requireNotIgnoringFallbacks();

        if (resolveStatus() == ResolveStatus.RESOLVED) {
            // falling back to a non-object doesn't merge anything, and also
            // prohibits merging any objects that we fall back to later.
            // so we have to switch to ignoresFallbacks mode.
            return withFallbacksIgnored();
        } else {
            // if unresolved, we may have to look back to fallbacks as part of
            // the resolution process, so always delay
            return delayMerge(stack, fallback);
        }
    }

    protected AbstractConfigValue mergedWithTheUnmergeable(Unmergeable fallback) {
        requireNotIgnoringFallbacks();

        return mergedWithTheUnmergeable(Collections.singletonList(this), fallback);
    }

    protected AbstractConfigValue mergedWithObject(AbstractConfigObject fallback) {
        requireNotIgnoringFallbacks();

        return mergedWithObject(Collections.singletonList(this), fallback);
    }

    protected AbstractConfigValue mergedWithNonObject(AbstractConfigValue fallback) {
        requireNotIgnoringFallbacks();

        return mergedWithNonObject(Collections.singletonList(this), fallback);
    }

    @Override
    public AbstractConfigValue withOrigin(ConfigOrigin origin) {
        if (this.origin == origin)
            return this;
        else
            return newCopy(origin);
    }

    // this is only overridden to change the return type
    @Override
    public AbstractConfigValue withFallback(ConfigMergeable mergeable) {
        if (ignoresFallbacks()) {
            return this;
        } else {
            ConfigValue other = ((MergeableValue) mergeable).toFallbackValue();

            if (other instanceof Unmergeable) {
                return mergedWithTheUnmergeable((Unmergeable) other);
            } else if (other instanceof AbstractConfigObject) {
                return mergedWithObject((AbstractConfigObject) other);
            } else {
                return mergedWithNonObject((AbstractConfigValue) other);
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof ConfigValue;
    }

    @Override
    public boolean equals(Object other) {
        // note that "origin" is deliberately NOT part of equality
        if (other instanceof ConfigValue) {
            return canEqual(other)
                    && (this.valueType() ==
                            ((ConfigValue) other).valueType())
                    && ConfigImplUtil.equalsHandlingNull(this.unwrapped(),
                            ((ConfigValue) other).unwrapped());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // note that "origin" is deliberately NOT part of equality
        Object o = this.unwrapped();
        if (o == null)
            return 0;
        else
            return o.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        render(sb, 0, true /* atRoot */, null /* atKey */, ConfigRenderOptions.concise());
        return getClass().getSimpleName() + "(" + sb.toString() + ")";
    }

    protected static void indent(StringBuilder sb, int indent, ConfigRenderOptions options) {
        if (options.getFormatted()) {
            int remaining = indent;
            while (remaining > 0) {
                sb.append("    ");
                --remaining;
            }
        }
    }

    protected void render(StringBuilder sb, int indent, boolean atRoot, String atKey, ConfigRenderOptions options) {
        if (atKey != null) {
            String renderedKey;
            if (options.getJson())
                renderedKey = ConfigImplUtil.renderJsonString(atKey);
            else
                renderedKey = ConfigImplUtil.renderStringUnquotedIfPossible(atKey);

            sb.append(renderedKey);

            if (options.getJson()) {
                if (options.getFormatted())
                    sb.append(" : ");
                else
                    sb.append(":");
            } else {
                // in non-JSON we can omit the colon or equals before an object
                if (this instanceof ConfigObject) {
                    if (options.getFormatted())
                        sb.append(' ');
                } else {
                    sb.append("=");
                }
            }
        }
        render(sb, indent, atRoot, options);
    }

    protected void render(StringBuilder sb, int indent, boolean atRoot, ConfigRenderOptions options) {
        Object u = unwrapped();
        sb.append(u.toString());
    }

    @Override
    public final String render() {
        return render(ConfigRenderOptions.defaults());
    }

    @Override
    public final String render(ConfigRenderOptions options) {
        StringBuilder sb = new StringBuilder();
        render(sb, 0, true, null, options);
        return sb.toString();
    }

    // toString() is a debugging-oriented string but this is defined
    // to create a string that would parse back to the value in JSON.
    // It only works for primitive values (that would be a single token)
    // which are auto-converted to strings when concatenating with
    // other strings or by the DefaultTransformer.
    String transformToString() {
        return null;
    }

    SimpleConfig atKey(ConfigOrigin origin, String key) {
        Map<String, AbstractConfigValue> m = Collections.singletonMap(key, this);
        return (new SimpleConfigObject(origin, m)).toConfig();
    }

    @Override
    public SimpleConfig atKey(String key) {
        return atKey(SimpleConfigOrigin.newSimple("atKey(" + key + ")"), key);
    }

    SimpleConfig atPath(ConfigOrigin origin, Path path) {
        Path parent = path.parent();
        SimpleConfig result = atKey(origin, path.last());
        while (parent != null) {
            String key = parent.last();
            result = result.atKey(origin, key);
            parent = parent.parent();
        }
        return result;
    }

    @Override
    public SimpleConfig atPath(String pathExpression) {
        SimpleConfigOrigin origin = SimpleConfigOrigin.newSimple("atPath(" + pathExpression + ")");
        return atPath(origin, Path.newPath(pathExpression));
    }
}
