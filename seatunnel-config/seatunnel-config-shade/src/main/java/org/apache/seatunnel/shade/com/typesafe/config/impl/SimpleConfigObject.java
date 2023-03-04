/*
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */

package org.apache.seatunnel.shade.com.typesafe.config.impl;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigOrigin;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class SimpleConfigObject extends AbstractConfigObject implements Serializable {
    private static final long serialVersionUID = 2L;
    private final Map<String, AbstractConfigValue> value;
    private final boolean resolved;
    private final boolean ignoresFallbacks;
    private static final SimpleConfigObject EMPTY_INSTANCE =
            empty(SimpleConfigOrigin.newSimple("empty config"));
    private static final int HASH_CODE = 41;

    SimpleConfigObject(
            ConfigOrigin origin,
            Map<String, AbstractConfigValue> value,
            ResolveStatus status,
            boolean ignoresFallbacks) {
        super(origin);
        if (value == null) {
            throw new ConfigException.BugOrBroken("creating config object with null map");
        } else {
            this.value = value;
            this.resolved = status == ResolveStatus.RESOLVED;
            this.ignoresFallbacks = ignoresFallbacks;
            if (status != ResolveStatus.fromValues(value.values())) {
                throw new ConfigException.BugOrBroken("Wrong resolved status on " + this);
            }
        }
    }

    SimpleConfigObject(ConfigOrigin origin, Map<String, AbstractConfigValue> value) {
        this(origin, value, ResolveStatus.fromValues(value.values()), false);
    }

    public SimpleConfigObject withOnlyKey(String key) {
        return this.withOnlyPath(Path.newKey(key));
    }

    public SimpleConfigObject withoutKey(String key) {
        return this.withoutPath(Path.newKey(key));
    }

    protected SimpleConfigObject withOnlyPathOrNull(Path path) {
        String key = path.first();
        Path next = path.remainder();
        AbstractConfigValue v = this.value.get(key);
        if (next != null) {
            if (v instanceof AbstractConfigObject) {
                v = ((AbstractConfigObject) v).withOnlyPathOrNull(next);
            } else {
                v = null;
            }
        }

        return v == null
                ? null
                : new SimpleConfigObject(
                        this.origin(),
                        Collections.singletonMap(key, v),
                        v.resolveStatus(),
                        this.ignoresFallbacks);
    }

    SimpleConfigObject withOnlyPath(Path path) {
        SimpleConfigObject o = this.withOnlyPathOrNull(path);
        return o == null
                ? new SimpleConfigObject(
                        this.origin(),
                        Collections.emptyMap(),
                        ResolveStatus.RESOLVED,
                        this.ignoresFallbacks)
                : o;
    }

    SimpleConfigObject withoutPath(Path path) {
        String key = path.first();
        Path next = path.remainder();
        AbstractConfigValue v = this.value.get(key);
        HashMap<String, AbstractConfigValue> smaller;
        if (next != null && v instanceof AbstractConfigObject) {
            v = ((AbstractConfigObject) v).withoutPath(next);
            smaller = new LinkedHashMap<>(this.value);
            smaller.put(key, v);
            return new SimpleConfigObject(
                    this.origin(),
                    smaller,
                    ResolveStatus.fromValues(smaller.values()),
                    this.ignoresFallbacks);
        } else if (next == null && v != null) {
            smaller = new LinkedHashMap<>(this.value.size() - 1);

            for (Entry<String, AbstractConfigValue> stringAbstractConfigValueEntry :
                    this.value.entrySet()) {
                if (!stringAbstractConfigValueEntry.getKey().equals(key)) {
                    smaller.put(
                            stringAbstractConfigValueEntry.getKey(),
                            stringAbstractConfigValueEntry.getValue());
                }
            }

            return new SimpleConfigObject(
                    this.origin(),
                    smaller,
                    ResolveStatus.fromValues(smaller.values()),
                    this.ignoresFallbacks);
        } else {
            return this;
        }
    }

    public SimpleConfigObject withValue(String key, ConfigValue v) {
        if (v == null) {
            throw new ConfigException.BugOrBroken(
                    "Trying to store null ConfigValue in a ConfigObject");
        } else {
            Map newMap;
            if (this.value.isEmpty()) {
                newMap = Collections.singletonMap(key, (AbstractConfigValue) v);
            } else {
                newMap = new LinkedHashMap<>(this.value);
                newMap.put(key, v);
            }

            return new SimpleConfigObject(
                    this.origin(),
                    newMap,
                    ResolveStatus.fromValues(newMap.values()),
                    this.ignoresFallbacks);
        }
    }

    SimpleConfigObject withValue(Path path, ConfigValue v) {
        String key = path.first();
        Path next = path.remainder();
        if (next == null) {
            return this.withValue(key, v);
        } else {
            AbstractConfigValue child = this.value.get(key);
            if (child instanceof AbstractConfigObject) {
                return this.withValue(key, ((AbstractConfigObject) child).withValue(next, v));
            } else {
                SimpleConfig subtree =
                        ((AbstractConfigValue) v)
                                .atPath(
                                        SimpleConfigOrigin.newSimple(
                                                "withValue(" + next.render() + ")"),
                                        next);
                return this.withValue(key, subtree.root());
            }
        }
    }

    protected AbstractConfigValue attemptPeekWithPartialResolve(String key) {
        return this.value.get(key);
    }

    private SimpleConfigObject newCopy(
            ResolveStatus newStatus, ConfigOrigin newOrigin, boolean newIgnoresFallbacks) {
        return new SimpleConfigObject(newOrigin, this.value, newStatus, newIgnoresFallbacks);
    }

    protected SimpleConfigObject newCopy(ResolveStatus newStatus, ConfigOrigin newOrigin) {
        return this.newCopy(newStatus, newOrigin, this.ignoresFallbacks);
    }

    protected SimpleConfigObject withFallbacksIgnored() {
        return this.ignoresFallbacks
                ? this
                : this.newCopy(this.resolveStatus(), this.origin(), true);
    }

    ResolveStatus resolveStatus() {
        return ResolveStatus.fromBoolean(this.resolved);
    }

    public SimpleConfigObject replaceChild(
            AbstractConfigValue child, AbstractConfigValue replacement) {
        Map<String, AbstractConfigValue> newChildren = new LinkedHashMap<>(this.value);
        Iterator<Entry<String, AbstractConfigValue>> var4 = newChildren.entrySet().iterator();

        Entry<String, AbstractConfigValue> old;
        do {
            if (!var4.hasNext()) {
                throw new ConfigException.BugOrBroken(
                        "SimpleConfigObject.replaceChild did not find " + child + " in " + this);
            }

            old = var4.next();
        } while (old.getValue() != child);

        if (replacement != null) {
            old.setValue(replacement);
        } else {
            newChildren.remove(old.getKey());
        }

        return new SimpleConfigObject(
                this.origin(),
                newChildren,
                ResolveStatus.fromValues(newChildren.values()),
                this.ignoresFallbacks);
    }

    public boolean hasDescendant(AbstractConfigValue descendant) {
        Iterator<AbstractConfigValue> var2 = this.value.values().iterator();

        AbstractConfigValue child;
        do {
            if (!var2.hasNext()) {
                var2 = this.value.values().iterator();

                do {
                    if (!var2.hasNext()) {
                        return false;
                    }

                    child = var2.next();
                } while (!(child instanceof Container)
                        || !((Container) child).hasDescendant(descendant));

                return true;
            }

            child = var2.next();
        } while (child != descendant);

        return true;
    }

    protected boolean ignoresFallbacks() {
        return this.ignoresFallbacks;
    }

    public Map<String, Object> unwrapped() {
        Map<String, Object> m = new LinkedHashMap<>();

        for (Entry<String, AbstractConfigValue> stringAbstractConfigValueEntry :
                this.value.entrySet()) {
            m.put(
                    stringAbstractConfigValueEntry.getKey(),
                    stringAbstractConfigValueEntry.getValue().unwrapped());
        }

        return m;
    }

    protected SimpleConfigObject mergedWithObject(AbstractConfigObject abstractFallback) {
        this.requireNotIgnoringFallbacks();
        if (!(abstractFallback instanceof SimpleConfigObject)) {
            throw new ConfigException.BugOrBroken(
                    "should not be reached (merging non-SimpleConfigObject)");
        } else {
            SimpleConfigObject fallback = (SimpleConfigObject) abstractFallback;
            boolean changed = false;
            boolean allResolved = true;
            Map<String, AbstractConfigValue> merged = new LinkedHashMap<>();
            Set<String> allKeys = new HashSet<>();
            allKeys.addAll(this.keySet());
            allKeys.addAll(fallback.keySet());

            for (String key : allKeys) {
                AbstractConfigValue first = this.value.get(key);
                AbstractConfigValue second = fallback.value.get(key);
                AbstractConfigValue kept;
                if (first == null) {
                    kept = second;
                } else if (second == null) {
                    kept = first;
                } else {
                    kept = first.withFallback(second);
                }

                merged.put(key, kept);
                if (first != kept) {
                    changed = true;
                }

                if (kept.resolveStatus() == ResolveStatus.UNRESOLVED) {
                    allResolved = false;
                }
            }

            ResolveStatus newResolveStatus = ResolveStatus.fromBoolean(allResolved);
            boolean newIgnoresFallbacks = fallback.ignoresFallbacks();
            if (changed) {
                return new SimpleConfigObject(
                        mergeOrigins(this, fallback),
                        merged,
                        newResolveStatus,
                        newIgnoresFallbacks);
            } else if (newResolveStatus == this.resolveStatus()
                    && newIgnoresFallbacks == this.ignoresFallbacks()) {
                return this;
            } else {
                return this.newCopy(newResolveStatus, this.origin(), newIgnoresFallbacks);
            }
        }
    }

    private SimpleConfigObject modify(NoExceptionsModifier modifier) {
        try {
            return this.modifyMayThrow(modifier);
        } catch (RuntimeException var3) {
            throw var3;
        } catch (Exception var4) {
            throw new ConfigException.BugOrBroken("unexpected checked exception", var4);
        }
    }

    private SimpleConfigObject modifyMayThrow(Modifier modifier) throws Exception {
        Map<String, AbstractConfigValue> changes = null;

        for (String k : this.keySet()) {
            AbstractConfigValue v = this.value.get(k);
            AbstractConfigValue modified = modifier.modifyChildMayThrow(k, v);
            if (modified != v) {
                if (changes == null) {
                    changes = new LinkedHashMap<>();
                }

                changes.put(k, modified);
            }
        }

        if (changes == null) {
            return this;
        } else {
            Map<String, AbstractConfigValue> modified = new LinkedHashMap<>();
            boolean sawUnresolved = false;

            for (String k : this.keySet()) {
                AbstractConfigValue newValue;
                if (changes.containsKey(k)) {
                    newValue = changes.get(k);
                    if (newValue != null) {
                        modified.put(k, newValue);
                        if (newValue.resolveStatus() == ResolveStatus.UNRESOLVED) {
                            sawUnresolved = true;
                        }
                    }
                } else {
                    newValue = this.value.get(k);
                    modified.put(k, newValue);
                    if (newValue.resolveStatus() == ResolveStatus.UNRESOLVED) {
                        sawUnresolved = true;
                    }
                }
            }

            return new SimpleConfigObject(
                    this.origin(),
                    modified,
                    sawUnresolved ? ResolveStatus.UNRESOLVED : ResolveStatus.RESOLVED,
                    this.ignoresFallbacks());
        }
    }

    ResolveResult<? extends AbstractConfigObject> resolveSubstitutions(
            ResolveContext context, ResolveSource source) throws NotPossibleToResolve {
        if (this.resolveStatus() == ResolveStatus.RESOLVED) {
            return ResolveResult.make(context, this);
        } else {
            ResolveSource sourceWithParent = source.pushParent(this);

            try {
                SimpleConfigObject.ResolveModifier modifier =
                        new SimpleConfigObject.ResolveModifier(context, sourceWithParent);
                AbstractConfigValue value = this.modifyMayThrow(modifier);
                return ResolveResult.make(modifier.context, value).asObjectResult();
            } catch (NotPossibleToResolve | RuntimeException var6) {
                throw var6;
            } catch (Exception var8) {
                throw new ConfigException.BugOrBroken("unexpected checked exception", var8);
            }
        }
    }

    SimpleConfigObject relativized(final Path prefix) {
        return this.modify(
                new NoExceptionsModifier() {
                    public AbstractConfigValue modifyChild(String key, AbstractConfigValue v) {
                        return v.relativized(prefix);
                    }
                });
    }

    protected void render(
            StringBuilder sb, int indent, boolean atRoot, ConfigRenderOptions options) {
        if (this.isEmpty()) {
            sb.append("{}");
        } else {
            boolean outerBraces = options.getJson() || !atRoot;
            int innerIndent;
            if (outerBraces) {
                innerIndent = indent + 1;
                sb.append("{");
                if (options.getFormatted()) {
                    sb.append('\n');
                }
            } else {
                innerIndent = indent;
            }

            int separatorCount = 0;
            String[] keys = this.keySet().toArray(new String[0]);

            for (String k : keys) {
                AbstractConfigValue v = this.value.get(k);
                if (options.getOriginComments()) {
                    String[] lines = v.origin().description().split("\n");

                    for (String l : lines) {
                        indent(sb, indent + 1, options);
                        sb.append('#');
                        if (!l.isEmpty()) {
                            sb.append(' ');
                        }

                        sb.append(l);
                        sb.append("\n");
                    }
                }

                if (options.getComments()) {

                    for (String comment : v.origin().comments()) {
                        indent(sb, innerIndent, options);
                        sb.append("#");
                        if (!comment.startsWith(" ")) {
                            sb.append(' ');
                        }

                        sb.append(comment);
                        sb.append("\n");
                    }
                }

                indent(sb, innerIndent, options);
                v.render(sb, innerIndent, false, k, options);
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

            sb.setLength(sb.length() - separatorCount);
            if (outerBraces) {
                if (options.getFormatted()) {
                    sb.append('\n');
                    indent(sb, indent, options);
                }

                sb.append("}");
            }
        }

        if (atRoot && options.getFormatted()) {
            sb.append('\n');
        }
    }

    public AbstractConfigValue get(Object key) {
        return this.value.get(key);
    }

    private static boolean mapEquals(Map<String, ConfigValue> a, Map<String, ConfigValue> b) {
        if (a == b) {
            return true;
        } else {
            Set<String> aKeys = a.keySet();
            Set<String> bKeys = b.keySet();
            if (aKeys.equals(bKeys)) {
                Iterator<String> var4 = aKeys.iterator();

                String key;
                do {
                    if (!var4.hasNext()) {
                        return true;
                    }

                    key = var4.next();
                } while (a.get(key).equals(b.get(key)));
            }
            return false;
        }
    }

    @SuppressWarnings("magicnumber")
    private static int mapHash(Map<String, ConfigValue> m) {
        List<String> keys = new ArrayList<>(m.keySet());
        Collections.sort(keys);
        int valuesHash = 0;

        String k;
        for (Iterator<String> var3 = keys.iterator();
                var3.hasNext();
                valuesHash += m.get(k).hashCode()) {
            k = var3.next();
        }

        return HASH_CODE * (HASH_CODE + keys.hashCode()) + valuesHash;
    }

    protected boolean canEqual(Object other) {
        return other instanceof ConfigObject;
    }

    public boolean equals(Object other) {
        if (!(other instanceof ConfigObject)) {
            return false;
        } else {
            return this.canEqual(other) && mapEquals(this, (ConfigObject) other);
        }
    }

    public int hashCode() {
        return mapHash(this);
    }

    public boolean containsKey(Object key) {
        return this.value.containsKey(key);
    }

    public Set<String> keySet() {
        return this.value.keySet();
    }

    public boolean containsValue(Object v) {
        return this.value.containsValue(v);
    }

    public Set<Entry<String, ConfigValue>> entrySet() {
        HashSet<Entry<String, ConfigValue>> entries = new HashSet<>();

        for (Entry<String, AbstractConfigValue> stringAbstractConfigValueEntry :
                this.value.entrySet()) {
            entries.add(
                    new AbstractMap.SimpleImmutableEntry<>(
                            stringAbstractConfigValueEntry.getKey(),
                            stringAbstractConfigValueEntry.getValue()));
        }

        return entries;
    }

    public boolean isEmpty() {
        return this.value.isEmpty();
    }

    public int size() {
        return this.value.size();
    }

    public Collection<ConfigValue> values() {
        return new HashSet<>(this.value.values());
    }

    static SimpleConfigObject empty() {
        return EMPTY_INSTANCE;
    }

    static SimpleConfigObject empty(ConfigOrigin origin) {
        return origin == null ? empty() : new SimpleConfigObject(origin, Collections.emptyMap());
    }

    static SimpleConfigObject emptyMissing(ConfigOrigin baseOrigin) {
        return new SimpleConfigObject(
                SimpleConfigOrigin.newSimple(baseOrigin.description() + " (not found)"),
                Collections.emptyMap());
    }

    private Object writeReplace() throws ObjectStreamException {
        return new SerializedConfigValue(this);
    }

    private static final class ResolveModifier implements Modifier {
        final Path originalRestrict;
        ResolveContext context;
        final ResolveSource source;

        ResolveModifier(ResolveContext context, ResolveSource source) {
            this.context = context;
            this.source = source;
            this.originalRestrict = context.restrictToChild();
        }

        public AbstractConfigValue modifyChildMayThrow(String key, AbstractConfigValue v)
                throws NotPossibleToResolve {
            if (this.context.isRestrictedToChild()) {
                if (key.equals(this.context.restrictToChild().first())) {
                    Path remainder = this.context.restrictToChild().remainder();
                    if (remainder != null) {
                        ResolveResult<? extends AbstractConfigValue> result =
                                this.context.restrict(remainder).resolve(v, this.source);
                        this.context =
                                result.context.unrestricted().restrict(this.originalRestrict);
                        return result.value;
                    } else {
                        return v;
                    }
                } else {
                    return v;
                }
            } else {
                ResolveResult<? extends AbstractConfigValue> result =
                        this.context.unrestricted().resolve(v, this.source);
                this.context = result.context.unrestricted().restrict(this.originalRestrict);
                return result.value;
            }
        }
    }
}
