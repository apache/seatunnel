package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.impl.AbstractConfigValue.NotPossibleToResolve;

/**
 * This class is the source for values for a substitution like ${foo}.
 */
final class ResolveSource {

    final AbstractConfigObject root;
    // This is used for knowing the chain of parents we used to get here.
    // null if we should assume we are not a descendant of the root.
    // the root itself should be a node in this if non-null.
    final Node<Container> pathFromRoot;

    ResolveSource(AbstractConfigObject root, Node<Container> pathFromRoot) {
        this.root = root;
        this.pathFromRoot = pathFromRoot;
    }

    ResolveSource(AbstractConfigObject root) {
        this.root = root;
        this.pathFromRoot = null;
    }

    // if we replace the root with a non-object, use an empty
    // object with nothing in it instead.
    private AbstractConfigObject rootMustBeObj(Container value) {
        if (value instanceof AbstractConfigObject) {
            return (AbstractConfigObject) value;
        } else {
            return SimpleConfigObject.empty();
        }
    }

    // as a side effect, findInObject() will have to resolve all parents of the
    // child being peeked, but NOT the child itself. Caller has to resolve
    // the child itself if needed. ValueWithPath.value can be null but
    // the ValueWithPath instance itself should not be.
    static private ResultWithPath findInObject(AbstractConfigObject obj, ResolveContext context, Path path)
            throws NotPossibleToResolve {
        // resolve ONLY portions of the object which are along our path
        if (ConfigImpl.traceSubstitutionsEnabled())
            ConfigImpl.trace("*** finding '" + path + "' in " + obj);
        Path restriction = context.restrictToChild();
        ResolveResult<? extends AbstractConfigValue> partiallyResolved = context.restrict(path).resolve(obj,
                new ResolveSource(obj));
        ResolveContext newContext = partiallyResolved.context.restrict(restriction);
        if (partiallyResolved.value instanceof AbstractConfigObject) {
            ValueWithPath pair = findInObject((AbstractConfigObject) partiallyResolved.value, path);
            return new ResultWithPath(ResolveResult.make(newContext, pair.value), pair.pathFromRoot);
        } else {
            throw new ConfigException.BugOrBroken("resolved object to non-object " + obj + " to " + partiallyResolved);
        }
    }

    static private ValueWithPath findInObject(AbstractConfigObject obj, Path path) {
        try {
            // we'll fail if anything along the path can't
            // be looked at without resolving.
            return findInObject(obj, path, null);
        } catch (ConfigException.NotResolved e) {
            throw ConfigImpl.improveNotResolved(path, e);
        }
    }

    static private ValueWithPath findInObject(AbstractConfigObject obj, Path path, Node<Container> parents) {
        String key = path.first();
        Path next = path.remainder();
        if (ConfigImpl.traceSubstitutionsEnabled())
            ConfigImpl.trace("*** looking up '" + key + "' in " + obj);
        AbstractConfigValue v = obj.attemptPeekWithPartialResolve(key);
        Node<Container> newParents = parents == null ? new Node<Container>(obj) : parents.prepend(obj);

        if (next == null) {
            return new ValueWithPath(v, newParents);
        } else {
            if (v instanceof AbstractConfigObject) {
                return findInObject((AbstractConfigObject) v, next, newParents);
            } else {
                return new ValueWithPath(null, newParents);
            }
        }
    }

    ResultWithPath lookupSubst(ResolveContext context, SubstitutionExpression subst,
            int prefixLength)
            throws NotPossibleToResolve {
        if (ConfigImpl.traceSubstitutionsEnabled())
            ConfigImpl.trace(context.depth(), "searching for " + subst);

        if (ConfigImpl.traceSubstitutionsEnabled())
            ConfigImpl.trace(context.depth(), subst + " - looking up relative to file it occurred in");
        // First we look up the full path, which means relative to the
        // included file if we were not a root file
        ResultWithPath result = findInObject(root, context, subst.path());

        if (result.result.value == null) {
            // Then we want to check relative to the root file. We don't
            // want the prefix we were included at to be used when looking
            // up env variables either.
            Path unprefixed = subst.path().subPath(prefixLength);

            if (prefixLength > 0) {
                if (ConfigImpl.traceSubstitutionsEnabled())
                    ConfigImpl.trace(result.result.context.depth(), unprefixed
                            + " - looking up relative to parent file");
                result = findInObject(root, result.result.context, unprefixed);
            }

            if (result.result.value == null && result.result.context.options().getUseSystemEnvironment()) {
                if (ConfigImpl.traceSubstitutionsEnabled())
                    ConfigImpl.trace(result.result.context.depth(), unprefixed + " - looking up in system environment");
                result = findInObject(ConfigImpl.envVariablesAsConfigObject(), context, unprefixed);
            }
        }

        if (ConfigImpl.traceSubstitutionsEnabled())
            ConfigImpl.trace(result.result.context.depth(), "resolved to " + result);

        return result;
    }

    ResolveSource pushParent(Container parent) {
        if (parent == null)
            throw new ConfigException.BugOrBroken("can't push null parent");

        if (ConfigImpl.traceSubstitutionsEnabled())
            ConfigImpl.trace("pushing parent " + parent + " ==root " + (parent == root) + " onto " + this);

        if (pathFromRoot == null) {
            if (parent == root) {
                return new ResolveSource(root, new Node<Container>(parent));
            } else {
                if (ConfigImpl.traceSubstitutionsEnabled()) {
                    // this hasDescendant check is super-expensive so it's a
                    // trace message rather than an assertion
                    if (root.hasDescendant((AbstractConfigValue) parent))
                        ConfigImpl.trace("***** BUG ***** tried to push parent " + parent
                                + " without having a path to it in " + this);
                }
                // ignore parents if we aren't proceeding from the
                // root
                return this;
            }
        } else {
            Container parentParent = pathFromRoot.head();
            if (ConfigImpl.traceSubstitutionsEnabled()) {
                // this hasDescendant check is super-expensive so it's a
                // trace message rather than an assertion
                if (parentParent != null && !parentParent.hasDescendant((AbstractConfigValue) parent))
                    ConfigImpl.trace("***** BUG ***** trying to push non-child of " + parentParent + ", non-child was "
                            + parent);
            }

            return new ResolveSource(root, pathFromRoot.prepend(parent));
        }
    }

    ResolveSource resetParents() {
        if (pathFromRoot == null)
            return this;
        else
            return new ResolveSource(root);
    }

    // returns null if the replacement results in deleting all the nodes.
    private static Node<Container> replace(Node<Container> list, Container old, AbstractConfigValue replacement) {
        Container child = list.head();
        if (child != old)
            throw new ConfigException.BugOrBroken("Can only replace() the top node we're resolving; had " + child
                    + " on top and tried to replace " + old + " overall list was " + list);
        Container parent = list.tail() == null ? null : list.tail().head();
        if (replacement == null || !(replacement instanceof Container)) {
            if (parent == null) {
                return null;
            } else {
                /*
                 * we are deleting the child from the stack of containers
                 * because it's either going away or not a container
                 */
                AbstractConfigValue newParent = parent.replaceChild((AbstractConfigValue) old, null);

                return replace(list.tail(), parent, newParent);
            }
        } else {
            /* we replaced the container with another container */
            if (parent == null) {
                return new Node<Container>((Container) replacement);
            } else {
                AbstractConfigValue newParent = parent.replaceChild((AbstractConfigValue) old, replacement);
                Node<Container> newTail = replace(list.tail(), parent, newParent);
                if (newTail != null)
                    return newTail.prepend((Container) replacement);
                else
                    return new Node<Container>((Container) replacement);
            }
        }
    }

    ResolveSource replaceCurrentParent(Container old, Container replacement) {
        if (ConfigImpl.traceSubstitutionsEnabled())
            ConfigImpl.trace("replaceCurrentParent old " + old + "@" + System.identityHashCode(old) + " replacement "
                + replacement + "@" + System.identityHashCode(old) + " in " + this);
        if (old == replacement) {
            return this;
        } else if (pathFromRoot != null) {
            Node<Container> newPath = replace(pathFromRoot, old, (AbstractConfigValue) replacement);
            if (ConfigImpl.traceSubstitutionsEnabled()) {
                ConfigImpl.trace("replaced " + old + " with " + replacement + " in " + this);
                ConfigImpl.trace("path was: " + pathFromRoot + " is now " + newPath);
            }
            // if we end up nuking the root object itself, we replace it with an
            // empty root
            if (newPath != null)
                return new ResolveSource((AbstractConfigObject) newPath.last(), newPath);
            else
                return new ResolveSource(SimpleConfigObject.empty());
        } else {
            if (old == root) {
                return new ResolveSource(rootMustBeObj(replacement));
            } else {
                throw new ConfigException.BugOrBroken("attempt to replace root " + root + " with " + replacement);
                // return this;
            }
        }
    }

    // replacement may be null to delete
    ResolveSource replaceWithinCurrentParent(AbstractConfigValue old, AbstractConfigValue replacement) {
        if (ConfigImpl.traceSubstitutionsEnabled())
            ConfigImpl.trace("replaceWithinCurrentParent old " + old + "@" + System.identityHashCode(old)
                    + " replacement " + replacement + "@" + System.identityHashCode(old) + " in " + this);
        if (old == replacement) {
            return this;
        } else if (pathFromRoot != null) {
            Container parent = pathFromRoot.head();
            AbstractConfigValue newParent = parent.replaceChild(old, replacement);
            return replaceCurrentParent(parent, (newParent instanceof Container) ? (Container) newParent : null);
        } else {
            if (old == root && replacement instanceof Container) {
                return new ResolveSource(rootMustBeObj((Container) replacement));
            } else {
//                throw new ConfigException.BugOrBroken("replace in parent not possible " + old + " with " + replacement
//                        + " in " + this);
                 return this;
            }
        }
    }

    @Override
    public String toString() {
        return "ResolveSource(root=" + root + ", pathFromRoot=" + pathFromRoot + ")";
    }

    // a persistent list
    static final class Node<T> {
        final T value;
        final Node<T> next;

        Node(T value, Node<T> next) {
            this.value = value;
            this.next = next;
        }

        Node(T value) {
            this(value, null);
        }

        Node<T> prepend(T value) {
            return new Node<T>(value, this);
        }

        T head() {
            return value;
        }

        Node<T> tail() {
            return next;
        }

        T last() {
            Node<T> i = this;
            while (i.next != null)
                i = i.next;
            return i.value;
        }

        Node<T> reverse() {
            if (next == null) {
                return this;
            } else {
                Node<T> reversed = new Node<T>(value);
                Node<T> i = next;
                while (i != null) {
                    reversed = reversed.prepend(i.value);
                    i = i.next;
                }
                return reversed;
            }
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("[");
            Node<T> toAppendValue = this.reverse();
            while (toAppendValue != null) {
                sb.append(toAppendValue.value.toString());
                if (toAppendValue.next != null)
                    sb.append(" <= ");
                toAppendValue = toAppendValue.next;
            }
            sb.append("]");
            return sb.toString();
        }
    }

    // value is allowed to be null
    static final class ValueWithPath {
        final AbstractConfigValue value;
        final Node<Container> pathFromRoot;

        ValueWithPath(AbstractConfigValue value, Node<Container> pathFromRoot) {
            this.value = value;
            this.pathFromRoot = pathFromRoot;
        }

        @Override
        public String toString() {
            return "ValueWithPath(value=" + value + ", pathFromRoot=" + pathFromRoot + ")";
        }
    }

    static final class ResultWithPath {
        final ResolveResult<? extends AbstractConfigValue> result;
        final Node<Container> pathFromRoot;

        ResultWithPath(ResolveResult<? extends AbstractConfigValue> result, Node<Container> pathFromRoot) {
            this.result = result;
            this.pathFromRoot = pathFromRoot;
        }

        @Override
        public String toString() {
            return "ResultWithPath(result=" + result + ", pathFromRoot=" + pathFromRoot + ")";
        }
    }
}
