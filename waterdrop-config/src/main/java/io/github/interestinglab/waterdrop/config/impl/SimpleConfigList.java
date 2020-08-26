/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigList;
import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigRenderOptions;
import io.github.interestinglab.waterdrop.config.ConfigValue;
import io.github.interestinglab.waterdrop.config.ConfigValueType;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

final class SimpleConfigList extends AbstractConfigValue implements ConfigList, Container, Serializable {

    private static final long serialVersionUID = 2L;

    final private List<AbstractConfigValue> value;
    final private boolean resolved;

    SimpleConfigList(ConfigOrigin origin, List<AbstractConfigValue> value) {
        this(origin, value, ResolveStatus
                .fromValues(value));
    }

    SimpleConfigList(ConfigOrigin origin, List<AbstractConfigValue> value,
            ResolveStatus status) {
        super(origin);
        this.value = value;
        this.resolved = status == ResolveStatus.RESOLVED;

        // kind of an expensive debug check (makes this constructor pointless)
        if (status != ResolveStatus.fromValues(value))
            throw new ConfigException.BugOrBroken(
                    "SimpleConfigList created with wrong resolve status: " + this);
    }

    @Override
    public ConfigValueType valueType() {
        return ConfigValueType.LIST;
    }

    @Override
    public List<Object> unwrapped() {
        List<Object> list = new ArrayList<Object>();
        for (AbstractConfigValue v : value) {
            list.add(v.unwrapped());
        }
        return list;
    }

    @Override
    ResolveStatus resolveStatus() {
        return ResolveStatus.fromBoolean(resolved);
    }

    @Override
    public SimpleConfigList replaceChild(AbstractConfigValue child, AbstractConfigValue replacement) {
        List<AbstractConfigValue> newList = replaceChildInList(value, child, replacement);
        if (newList == null) {
            return null;
        } else {
            // we use the constructor flavor that will recompute the resolve
            // status
            return new SimpleConfigList(origin(), newList);
        }
    }

    @Override
    public boolean hasDescendant(AbstractConfigValue descendant) {
        return hasDescendantInList(value, descendant);
    }

    private SimpleConfigList modify(NoExceptionsModifier modifier, ResolveStatus newResolveStatus) {
        try {
            return modifyMayThrow(modifier, newResolveStatus);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException.BugOrBroken("unexpected checked exception", e);
        }
    }

    private SimpleConfigList modifyMayThrow(Modifier modifier, ResolveStatus newResolveStatus)
            throws Exception {
        // lazy-create for optimization
        List<AbstractConfigValue> changed = null;
        int i = 0;
        for (AbstractConfigValue v : value) {
            AbstractConfigValue modified = modifier.modifyChildMayThrow(null /* key */, v);

            // lazy-create the new list if required
            if (changed == null && modified != v) {
                changed = new ArrayList<AbstractConfigValue>();
                for (int j = 0; j < i; ++j) {
                    changed.add(value.get(j));
                }
            }

            // once the new list is created, all elements
            // have to go in it. if modifyChild returned
            // null, we drop that element.
            if (changed != null && modified != null) {
                changed.add(modified);
            }

            i += 1;
        }

        if (changed != null) {
            if (newResolveStatus != null) {
                return new SimpleConfigList(origin(), changed, newResolveStatus);
            } else {
                return new SimpleConfigList(origin(), changed);
            }
        } else {
            return this;
        }
    }

    private static class ResolveModifier implements Modifier {
        ResolveContext context;
        final ResolveSource source;
        ResolveModifier(ResolveContext context, ResolveSource source) {
            this.context = context;
            this.source = source;
        }

        @Override
        public AbstractConfigValue modifyChildMayThrow(String key, AbstractConfigValue v)
                    throws NotPossibleToResolve {
            ResolveResult<? extends AbstractConfigValue> result = context.resolve(v, source);
            context = result.context;
            return result.value;
            }
    }

    @Override
    ResolveResult<? extends SimpleConfigList> resolveSubstitutions(ResolveContext context, ResolveSource source)
            throws NotPossibleToResolve {
        if (resolved)
            return ResolveResult.make(context, this);

        if (context.isRestrictedToChild()) {
            // if a list restricts to a child path, then it has no child paths,
            // so nothing to do.
            return ResolveResult.make(context, this);
        } else {
            try {
                ResolveModifier modifier = new ResolveModifier(context, source.pushParent(this));
                SimpleConfigList value = modifyMayThrow(modifier, context.options().getAllowUnresolved() ? null : ResolveStatus.RESOLVED);
                return ResolveResult.make(modifier.context, value);
            } catch (NotPossibleToResolve e) {
                throw e;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new ConfigException.BugOrBroken("unexpected checked exception", e);
            }
        }
    }

    @Override
    SimpleConfigList relativized(final Path prefix) {
        return modify(new NoExceptionsModifier() {
            @Override
            public AbstractConfigValue modifyChild(String key, AbstractConfigValue v) {
                return v.relativized(prefix);
            }

        }, resolveStatus());
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof SimpleConfigList;
    }

    @Override
    public boolean equals(Object other) {
        // note that "origin" is deliberately NOT part of equality
        if (other instanceof SimpleConfigList) {
            // optimization to avoid unwrapped() for two ConfigList
            return canEqual(other)
                    && (value == ((SimpleConfigList) other).value || value.equals(((SimpleConfigList) other).value));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // note that "origin" is deliberately NOT part of equality
        return value.hashCode();
    }

    @Override
    protected void render(StringBuilder sb, int indent, boolean atRoot, ConfigRenderOptions options) {
        if (value.isEmpty()) {
            sb.append("[]");
        } else {
            sb.append("[");
            if (options.getFormatted())
                sb.append('\n');
            for (AbstractConfigValue v : value) {
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
                        indent(sb, indent + 1, options);
                        sb.append("# ");
                        sb.append(comment);
                        sb.append("\n");
                    }
                }
                indent(sb, indent + 1, options);

                v.render(sb, indent + 1, atRoot, options);
                sb.append(",");
                if (options.getFormatted())
                    sb.append('\n');
            }
            sb.setLength(sb.length() - 1); // chop or newline
            if (options.getFormatted()) {
                sb.setLength(sb.length() - 1); // also chop comma
                sb.append('\n');
                indent(sb, indent, options);
            }
            sb.append("]");
        }
    }

    @Override
    public boolean contains(Object o) {
        return value.contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return value.containsAll(c);
    }

    @Override
    public AbstractConfigValue get(int index) {
        return value.get(index);
    }

    @Override
    public int indexOf(Object o) {
        return value.indexOf(o);
    }

    @Override
    public boolean isEmpty() {
        return value.isEmpty();
    }

    @Override
    public Iterator<ConfigValue> iterator() {
        final Iterator<AbstractConfigValue> i = value.iterator();

        return new Iterator<ConfigValue>() {
            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public ConfigValue next() {
                return i.next();
            }

            @Override
            public void remove() {
                throw weAreImmutable("iterator().remove");
            }
        };
    }

    @Override
    public int lastIndexOf(Object o) {
        return value.lastIndexOf(o);
    }

    private static ListIterator<ConfigValue> wrapListIterator(
            final ListIterator<AbstractConfigValue> i) {
        return new ListIterator<ConfigValue>() {
            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public ConfigValue next() {
                return i.next();
            }

            @Override
            public void remove() {
                throw weAreImmutable("listIterator().remove");
            }

            @Override
            public void add(ConfigValue arg0) {
                throw weAreImmutable("listIterator().add");
            }

            @Override
            public boolean hasPrevious() {
                return i.hasPrevious();
            }

            @Override
            public int nextIndex() {
                return i.nextIndex();
            }

            @Override
            public ConfigValue previous() {
                return i.previous();
            }

            @Override
            public int previousIndex() {
                return i.previousIndex();
            }

            @Override
            public void set(ConfigValue arg0) {
                throw weAreImmutable("listIterator().set");
            }
        };
    }

    @Override
    public ListIterator<ConfigValue> listIterator() {
        return wrapListIterator(value.listIterator());
    }

    @Override
    public ListIterator<ConfigValue> listIterator(int index) {
        return wrapListIterator(value.listIterator(index));
    }

    @Override
    public int size() {
        return value.size();
    }

    @Override
    public List<ConfigValue> subList(int fromIndex, int toIndex) {
        List<ConfigValue> list = new ArrayList<ConfigValue>();
        // yay bloat caused by lack of type variance
        for (AbstractConfigValue v : value.subList(fromIndex, toIndex)) {
            list.add(v);
        }
        return list;
    }

    @Override
    public Object[] toArray() {
        return value.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return value.toArray(a);
    }

    private static UnsupportedOperationException weAreImmutable(String method) {
        return new UnsupportedOperationException(
                "ConfigList is immutable, you can't call List.'" + method + "'");
    }

    @Override
    public boolean add(ConfigValue e) {
        throw weAreImmutable("add");
    }

    @Override
    public void add(int index, ConfigValue element) {
        throw weAreImmutable("add");
    }

    @Override
    public boolean addAll(Collection<? extends ConfigValue> c) {
        throw weAreImmutable("addAll");
    }

    @Override
    public boolean addAll(int index, Collection<? extends ConfigValue> c) {
        throw weAreImmutable("addAll");
    }

    @Override
    public void clear() {
        throw weAreImmutable("clear");
    }

    @Override
    public boolean remove(Object o) {
        throw weAreImmutable("remove");
    }

    @Override
    public ConfigValue remove(int index) {
        throw weAreImmutable("remove");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw weAreImmutable("removeAll");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw weAreImmutable("retainAll");
    }

    @Override
    public ConfigValue set(int index, ConfigValue element) {
        throw weAreImmutable("set");
    }

    @Override
    protected SimpleConfigList newCopy(ConfigOrigin newOrigin) {
        return new SimpleConfigList(newOrigin, value);
    }

    final SimpleConfigList concatenate(SimpleConfigList other) {
        ConfigOrigin combinedOrigin = SimpleConfigOrigin.mergeOrigins(origin(), other.origin());
        List<AbstractConfigValue> combined = new ArrayList<AbstractConfigValue>(value.size()
                + other.value.size());
        combined.addAll(value);
        combined.addAll(other.value);
        return new SimpleConfigList(combinedOrigin, combined);
    }

    // serialization all goes through SerializedConfigValue
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedConfigValue(this);
    }

    @Override
    public SimpleConfigList withOrigin(ConfigOrigin origin) {
        return (SimpleConfigList) super.withOrigin(origin);
    }
}
