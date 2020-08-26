/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigObject;
import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigRenderOptions;
import io.github.interestinglab.waterdrop.config.ConfigValue;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class SimpleConfigObject extends AbstractConfigObject implements Serializable {

    private static final long serialVersionUID = 2L;

    // this map should never be modified - assume immutable
    final private Map<String, AbstractConfigValue> value;
    final private boolean resolved;
    final private boolean ignoresFallbacks;

    SimpleConfigObject(ConfigOrigin origin,
                       Map<String, AbstractConfigValue> value, ResolveStatus status,
                       boolean ignoresFallbacks) {
        super(origin);
        if (value == null)
            throw new ConfigException.BugOrBroken(
                    "creating config object with null map");
        this.value = value;
        this.resolved = status == ResolveStatus.RESOLVED;
        this.ignoresFallbacks = ignoresFallbacks;

        // Kind of an expensive debug check. Comment out?
        if (status != ResolveStatus.fromValues(value.values()))
            throw new ConfigException.BugOrBroken("Wrong resolved status on " + this);
    }

    SimpleConfigObject(ConfigOrigin origin,
            Map<String, AbstractConfigValue> value) {
        this(origin, value, ResolveStatus.fromValues(value.values()), false /* ignoresFallbacks */);
    }

    @Override
    public SimpleConfigObject withOnlyKey(String key) {
        return withOnlyPath(Path.newKey(key));
    }

    @Override
    public SimpleConfigObject withoutKey(String key) {
        return withoutPath(Path.newKey(key));
    }

    // gets the object with only the path if the path
    // exists, otherwise null if it doesn't. this ensures
    // that if we have { a : { b : 42 } } and do
    // withOnlyPath("a.b.c") that we don't keep an empty
    // "a" object.
    @Override
    protected SimpleConfigObject withOnlyPathOrNull(Path path) {
        String key = path.first();
        Path next = path.remainder();
        AbstractConfigValue v = value.get(key);

        if (next != null) {
            if (v != null && (v instanceof AbstractConfigObject)) {
                v = ((AbstractConfigObject) v).withOnlyPathOrNull(next);
            } else {
                // if the path has more elements but we don't have an object,
                // then the rest of the path does not exist.
                v = null;
            }
        }

        if (v == null) {
            return null;
        } else {
            return new SimpleConfigObject(origin(), Collections.singletonMap(key, v),
                    v.resolveStatus(), ignoresFallbacks);
        }
    }

    @Override
    SimpleConfigObject withOnlyPath(Path path) {
        SimpleConfigObject o = withOnlyPathOrNull(path);
        if (o == null) {
            return new SimpleConfigObject(origin(),
                    Collections.<String, AbstractConfigValue> emptyMap(), ResolveStatus.RESOLVED,
                    ignoresFallbacks);
        } else {
            return o;
        }
    }

    @Override
    SimpleConfigObject withoutPath(Path path) {
        String key = path.first();
        Path next = path.remainder();
        AbstractConfigValue v = value.get(key);

        if (v != null && next != null && v instanceof AbstractConfigObject) {
            v = ((AbstractConfigObject) v).withoutPath(next);
            Map<String, AbstractConfigValue> updated = new LinkedHashMap<String, AbstractConfigValue>(
                    value);
            updated.put(key, v);
            return new SimpleConfigObject(origin(), updated, ResolveStatus.fromValues(updated
                    .values()), ignoresFallbacks);
        } else if (next != null || v == null) {
            // can't descend, nothing to remove
            return this;
        } else {
            Map<String, AbstractConfigValue> smaller = new LinkedHashMap<String, AbstractConfigValue>(
                    value.size() - 1);
            for (Map.Entry<String, AbstractConfigValue> old : value.entrySet()) {
                if (!old.getKey().equals(key))
                    smaller.put(old.getKey(), old.getValue());
            }
            return new SimpleConfigObject(origin(), smaller, ResolveStatus.fromValues(smaller
                    .values()), ignoresFallbacks);
        }
    }

    @Override
    public SimpleConfigObject withValue(String key, ConfigValue v) {
        if (v == null)
            throw new ConfigException.BugOrBroken(
                    "Trying to store null ConfigValue in a ConfigObject");

        Map<String, AbstractConfigValue> newMap;
        if (value.isEmpty()) {
            newMap = Collections.singletonMap(key, (AbstractConfigValue) v);
        } else {
            newMap = new LinkedHashMap<String, AbstractConfigValue>(value);
            newMap.put(key, (AbstractConfigValue) v);
        }

        return new SimpleConfigObject(origin(), newMap, ResolveStatus.fromValues(newMap.values()),
                ignoresFallbacks);
    }

    @Override
    SimpleConfigObject withValue(Path path, ConfigValue v) {
        String key = path.first();
        Path next = path.remainder();

        if (next == null) {
            return withValue(key, v);
        } else {
            AbstractConfigValue child = value.get(key);
            if (child != null && child instanceof AbstractConfigObject) {
                // if we have an object, add to it
                return withValue(key, ((AbstractConfigObject) child).withValue(next, v));
            } else {
                // as soon as we have a non-object, replace it entirely
                SimpleConfig subtree = ((AbstractConfigValue) v).atPath(
                        SimpleConfigOrigin.newSimple("withValue(" + next.render() + ")"), next);
                return withValue(key, subtree.root());
            }
        }
    }

    @Override
    protected AbstractConfigValue attemptPeekWithPartialResolve(String key) {
        return value.get(key);
    }

    private SimpleConfigObject newCopy(ResolveStatus newStatus, ConfigOrigin newOrigin,
            boolean newIgnoresFallbacks) {
        return new SimpleConfigObject(newOrigin, value, newStatus, newIgnoresFallbacks);
    }

    @Override
    protected SimpleConfigObject newCopy(ResolveStatus newStatus, ConfigOrigin newOrigin) {
        return newCopy(newStatus, newOrigin, ignoresFallbacks);
    }

    @Override
    protected SimpleConfigObject withFallbacksIgnored() {
        if (ignoresFallbacks)
            return this;
        else
            return newCopy(resolveStatus(), origin(), true /* ignoresFallbacks */);
    }

    @Override
    ResolveStatus resolveStatus() {
        return ResolveStatus.fromBoolean(resolved);
    }

    @Override
    public SimpleConfigObject replaceChild(AbstractConfigValue child, AbstractConfigValue replacement) {
        LinkedHashMap<String, AbstractConfigValue> newChildren = new LinkedHashMap<String, AbstractConfigValue>(value);
        for (Map.Entry<String, AbstractConfigValue> old : newChildren.entrySet()) {
            if (old.getValue() == child) {
                if (replacement != null)
                    old.setValue(replacement);
                else
                    newChildren.remove(old.getKey());

                return new SimpleConfigObject(origin(), newChildren, ResolveStatus.fromValues(newChildren.values()),
                        ignoresFallbacks);
            }
        }
        throw new ConfigException.BugOrBroken("SimpleConfigObject.replaceChild did not find " + child + " in " + this);
    }

    @Override
    public boolean hasDescendant(AbstractConfigValue descendant) {
        for (AbstractConfigValue child : value.values()) {
            if (child == descendant)
                return true;
        }
        // now do the expensive search
        for (AbstractConfigValue child : value.values()) {
            if (child instanceof Container && ((Container) child).hasDescendant(descendant))
                return true;
        }

        return false;
    }

    @Override
    protected boolean ignoresFallbacks() {
        return ignoresFallbacks;
    }

    @Override
    public Map<String, Object> unwrapped() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, AbstractConfigValue> e : value.entrySet()) {
            m.put(e.getKey(), e.getValue().unwrapped());
        }
        return m;
    }

    @Override
    protected SimpleConfigObject mergedWithObject(AbstractConfigObject abstractFallback) {
        requireNotIgnoringFallbacks();

        if (!(abstractFallback instanceof SimpleConfigObject)) {
            throw new ConfigException.BugOrBroken(
                    "should not be reached (merging non-SimpleConfigObject)");
        }

        SimpleConfigObject fallback = (SimpleConfigObject) abstractFallback;

        boolean changed = false;
        boolean allResolved = true;
        Map<String, AbstractConfigValue> merged = new LinkedHashMap<String, AbstractConfigValue>();
        Set<String> allKeys = new HashSet<String>();
        allKeys.addAll(this.keySet());
        allKeys.addAll(fallback.keySet());
        for (String key : allKeys) {
            AbstractConfigValue first = this.value.get(key);
            AbstractConfigValue second = fallback.value.get(key);
            AbstractConfigValue kept;
            if (first == null)
                kept = second;
            else if (second == null)
                kept = first;
            else
                kept = first.withFallback(second);

            merged.put(key, kept);

            if (first != kept)
                changed = true;

            if (kept.resolveStatus() == ResolveStatus.UNRESOLVED)
                allResolved = false;
        }

        ResolveStatus newResolveStatus = ResolveStatus.fromBoolean(allResolved);
        boolean newIgnoresFallbacks = fallback.ignoresFallbacks();

        if (changed)
            return new SimpleConfigObject(mergeOrigins(this, fallback), merged, newResolveStatus,
                    newIgnoresFallbacks);
        else if (newResolveStatus != resolveStatus() || newIgnoresFallbacks != ignoresFallbacks())
            return newCopy(newResolveStatus, origin(), newIgnoresFallbacks);
        else
            return this;
    }

    private SimpleConfigObject modify(NoExceptionsModifier modifier) {
        try {
            return modifyMayThrow(modifier);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException.BugOrBroken("unexpected checked exception", e);
        }
    }

    private SimpleConfigObject modifyMayThrow(Modifier modifier) throws Exception {
        Map<String, AbstractConfigValue> changes = null;
        for (String k : keySet()) {
            AbstractConfigValue v = value.get(k);
            // "modified" may be null, which means remove the child;
            // to do that we put null in the "changes" map.
            AbstractConfigValue modified = modifier.modifyChildMayThrow(k, v);
            if (modified != v) {
                if (changes == null)
                    changes = new LinkedHashMap<String, AbstractConfigValue>();
                changes.put(k, modified);
            }
        }
        if (changes == null) {
            return this;
        } else {
            Map<String, AbstractConfigValue> modified = new LinkedHashMap<String, AbstractConfigValue>();
            boolean sawUnresolved = false;
            for (String k : keySet()) {
                if (changes.containsKey(k)) {
                    AbstractConfigValue newValue = changes.get(k);
                    if (newValue != null) {
                        modified.put(k, newValue);
                        if (newValue.resolveStatus() == ResolveStatus.UNRESOLVED)
                            sawUnresolved = true;
                    } else {
                        // remove this child; don't put it in the new map.
                    }
                } else {
                    AbstractConfigValue newValue = value.get(k);
                    modified.put(k, newValue);
                    if (newValue.resolveStatus() == ResolveStatus.UNRESOLVED)
                        sawUnresolved = true;
                }
            }
            return new SimpleConfigObject(origin(), modified,
                    sawUnresolved ? ResolveStatus.UNRESOLVED : ResolveStatus.RESOLVED,
                    ignoresFallbacks());
        }
    }

    private static final class ResolveModifier implements Modifier {

        final Path originalRestrict;
        ResolveContext context;
        final ResolveSource source;

        ResolveModifier(ResolveContext context, ResolveSource source) {
            this.context = context;
            this.source = source;
            originalRestrict = context.restrictToChild();
        }

        @Override
        public AbstractConfigValue modifyChildMayThrow(String key, AbstractConfigValue v) throws NotPossibleToResolve {
            if (context.isRestrictedToChild()) {
                if (key.equals(context.restrictToChild().first())) {
                    Path remainder = context.restrictToChild().remainder();
                    if (remainder != null) {
                        ResolveResult<? extends AbstractConfigValue> result = context.restrict(remainder).resolve(v,
                                source);
                        context = result.context.unrestricted().restrict(originalRestrict);
                        return result.value;
                    } else {
                        // we don't want to resolve the leaf child.
                        return v;
                    }
                } else {
                    // not in the restrictToChild path
                    return v;
                }
            } else {
                // no restrictToChild, resolve everything
                ResolveResult<? extends AbstractConfigValue> result = context.unrestricted().resolve(v, source);
                context = result.context.unrestricted().restrict(originalRestrict);
                return result.value;
            }
        }

    }

    @Override
    ResolveResult<? extends AbstractConfigObject> resolveSubstitutions(ResolveContext context, ResolveSource source)
            throws NotPossibleToResolve {
        if (resolveStatus() == ResolveStatus.RESOLVED)
            return ResolveResult.make(context, this);

        final ResolveSource sourceWithParent = source.pushParent(this);

        try {
            ResolveModifier modifier = new ResolveModifier(context, sourceWithParent);

            AbstractConfigValue value = modifyMayThrow(modifier);
            return ResolveResult.make(modifier.context, value).asObjectResult();
        } catch (NotPossibleToResolve e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException.BugOrBroken("unexpected checked exception", e);
        }
    }

    @Override
    SimpleConfigObject relativized(final Path prefix) {
        return modify(new NoExceptionsModifier() {

            @Override
            public AbstractConfigValue modifyChild(String key, AbstractConfigValue v) {
                return v.relativized(prefix);
            }

        });
    }

    // this is only Serializable to chill out a findbugs warning
    static final private class RenderComparator implements java.util.Comparator<String>, Serializable {
        private static final long serialVersionUID = 1L;

        private static boolean isAllDigits(String s) {
            int length = s.length();

            // empty string doesn't count as a number
            if (length == 0)
                return false;

            for (int i = 0; i < length; ++i) {
                char c = s.charAt(i);

                if (Character.isDigit(c))
                    continue;
                else
                    return false;
            }
            return true;
        }

        // This is supposed to sort numbers before strings,
        // and sort the numbers numerically. The point is
        // to make objects which are really list-like
        // (numeric indices) appear in order.
        @Override
        public int compare(String a, String b) {
            boolean aDigits = isAllDigits(a);
            boolean bDigits = isAllDigits(b);
            if (aDigits && bDigits) {
                return Integer.compare(Integer.parseInt(a), Integer.parseInt(b));
            } else if (aDigits) {
                return -1;
            } else if (bDigits) {
                return 1;
            } else {
                return a.compareTo(b);
            }
        }
    }

    @Override
    protected void render(StringBuilder sb, int indent, boolean atRoot, ConfigRenderOptions options) {
        if (isEmpty()) {
            sb.append("{}");
        } else {
            boolean outerBraces = options.getJson() || !atRoot;

            int innerIndent;
            if (outerBraces) {
                innerIndent = indent + 1;
                sb.append("{");

                if (options.getFormatted())
                    sb.append('\n');
            } else {
                innerIndent = indent;
            }

            int separatorCount = 0;
            String[] keys = keySet().toArray(new String[size()]);
            // Arrays.sort(keys, new RenderComparator()); // we do not sort keys to keep key insertion order !!! Maybe we can make this configurable later
            for (String k : keys) {

                AbstractConfigValue v;
                v = value.get(k);

                if (options.getOriginComments()) {
                    String[] lines = v.origin().description().split("\n");
                    for (String l : lines) {
                        indent(sb, indent + 1, options);
                        sb.append('#');
                        if (!l.isEmpty())
                            sb.append(' ');
                        sb.append(l);
                        sb.append("\n");
                    }
                }
                if (options.getComments()) {
                    for (String comment : v.origin().comments()) {
                        indent(sb, innerIndent, options);
                        sb.append("#");
                        if (!comment.startsWith(" "))
                            sb.append(' ');
                        sb.append(comment);
                        sb.append("\n");
                    }
                }
                indent(sb, innerIndent, options);
                v.render(sb, innerIndent, false /* atRoot */, k, options);

                if (options.getFormatted()) {
                    if (options.getJson()) {
                        sb.append(",");
                        separatorCount = 2;
                    } else {
                        separatorCount = 1;
                    }
                    sb.append('\n');
                } else {
                    sb.append(",");
                    separatorCount = 1;
                }
            }
            // chop last commas/newlines
            sb.setLength(sb.length() - separatorCount);

            if (outerBraces) {
                if (options.getFormatted()) {
                    sb.append('\n'); // put a newline back
                    if (outerBraces)
                        indent(sb, indent, options);
                }
                sb.append("}");
            }
        }
        if (atRoot && options.getFormatted())
            sb.append('\n');
    }

    @Override
    public AbstractConfigValue get(Object key) {
        return value.get(key);
    }

    private static boolean mapEquals(Map<String, ConfigValue> a, Map<String, ConfigValue> b) {
        if (a == b)
            return true;

        Set<String> aKeys = a.keySet();
        Set<String> bKeys = b.keySet();

        if (!aKeys.equals(bKeys))
            return false;

        for (String key : aKeys) {
            if (!a.get(key).equals(b.get(key)))
                return false;
        }
        return true;
    }

    private static int mapHash(Map<String, ConfigValue> m) {
        // the keys have to be sorted, otherwise we could be equal
        // to another map but have a different hashcode.
        List<String> keys = new ArrayList<String>();
        keys.addAll(m.keySet());
        Collections.sort(keys);

        int valuesHash = 0;
        for (String k : keys) {
            valuesHash += m.get(k).hashCode();
        }
        return 41 * (41 + keys.hashCode()) + valuesHash;
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof ConfigObject;
    }

    @Override
    public boolean equals(Object other) {
        // note that "origin" is deliberately NOT part of equality.
        // neither are other "extras" like ignoresFallbacks or resolve status.
        if (other instanceof ConfigObject) {
            // optimization to avoid unwrapped() for two ConfigObject,
            // which is what AbstractConfigValue does.
            return canEqual(other) && mapEquals(this, ((ConfigObject) other));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // note that "origin" is deliberately NOT part of equality
        // neither are other "extras" like ignoresFallbacks or resolve status.
        return mapHash(this);
    }

    @Override
    public boolean containsKey(Object key) {
        return value.containsKey(key);
    }

    @Override
    public Set<String> keySet() {
        return value.keySet();
    }

    @Override
    public boolean containsValue(Object v) {
        return value.containsValue(v);
    }

    @Override
    public Set<Map.Entry<String, ConfigValue>> entrySet() {
        // total bloat just to work around lack of type variance

        HashSet<Map.Entry<String, ConfigValue>> entries = new HashSet<Map.Entry<String, ConfigValue>>();
        for (Map.Entry<String, AbstractConfigValue> e : value.entrySet()) {
            entries.add(new AbstractMap.SimpleImmutableEntry<String, ConfigValue>(
                    e.getKey(), e
                    .getValue()));
        }
        return entries;
    }

    @Override
    public boolean isEmpty() {
        return value.isEmpty();
    }

    @Override
    public int size() {
        return value.size();
    }

    @Override
    public Collection<ConfigValue> values() {
        return new HashSet<ConfigValue>(value.values());
    }

    final private static String EMPTY_NAME = "empty config";
    final private static SimpleConfigObject emptyInstance = empty(SimpleConfigOrigin
            .newSimple(EMPTY_NAME));

    final static SimpleConfigObject empty() {
        return emptyInstance;
    }

    final static SimpleConfigObject empty(ConfigOrigin origin) {
        if (origin == null)
            return empty();
        else
            return new SimpleConfigObject(origin,
                    Collections.<String, AbstractConfigValue> emptyMap());
    }

    final static SimpleConfigObject emptyMissing(ConfigOrigin baseOrigin) {
        return new SimpleConfigObject(SimpleConfigOrigin.newSimple(
                baseOrigin.description() + " (not found)"),
                Collections.<String, AbstractConfigValue> emptyMap());
    }

    // serialization all goes through SerializedConfigValue
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedConfigValue(this);
    }
}
