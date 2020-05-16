/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

import io.github.interestinglab.waterdrop.config.impl.ConfigImplUtil;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * All exceptions thrown by the library are subclasses of
 * <code>ConfigException</code>.
 */
public abstract class ConfigException extends RuntimeException implements Serializable {
    private static final long serialVersionUID = 1L;

    final private transient ConfigOrigin origin;

    protected ConfigException(ConfigOrigin origin, String message,
            Throwable cause) {
        super(origin.description() + ": " + message, cause);
        this.origin = origin;
    }

    protected ConfigException(ConfigOrigin origin, String message) {
        this(origin.description() + ": " + message, null);
    }

    protected ConfigException(String message, Throwable cause) {
        super(message, cause);
        this.origin = null;
    }

    protected ConfigException(String message) {
        this(message, null);
    }

    /**
     * Returns an "origin" (such as a filename and line number) for the
     * exception, or null if none is available. If there's no sensible origin
     * for a given exception, or the kind of exception doesn't meaningfully
     * relate to a particular origin file, this returns null. Never assume this
     * will return non-null, it can always return null.
     *
     * @return origin of the problem, or null if unknown/inapplicable
     */
    public ConfigOrigin origin() {
        return origin;
    }

    // we customize serialization because ConfigOrigin isn't
    // serializable and we don't want it to be (don't want to
    // support it)
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        ConfigImplUtil.writeOrigin(out, origin);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
      ClassNotFoundException {
        in.defaultReadObject();
        ConfigOrigin origin = ConfigImplUtil.readOrigin(in);
        // circumvent "final"
        Field f;
        try {
            f = ConfigException.class.getDeclaredField("origin");
        } catch (NoSuchFieldException e) {
            throw new IOException("ConfigException has no origin field?", e);
        } catch (SecurityException e) {
            throw new IOException("unable to fill out origin field in ConfigException", e);
        }
        f.setAccessible(true);
        try {
            f.set(this, origin);
        } catch (IllegalArgumentException e) {
            throw new IOException("unable to set origin field", e);
        } catch (IllegalAccessException e) {
            throw new IOException("unable to set origin field", e);
        }
    }

    /**
     * Exception indicating that the type of a value does not match the type you
     * requested.
     *
     */
    public static class WrongType extends ConfigException {
        private static final long serialVersionUID = 1L;

        public WrongType(ConfigOrigin origin, String path, String expected, String actual,
                         Throwable cause) {
            super(origin, path + " has type " + actual + " rather than " + expected, cause);
        }

        public WrongType(ConfigOrigin origin, String path, String expected, String actual) {
            this(origin, path, expected, actual, null);
        }

        public WrongType(ConfigOrigin origin, String message, Throwable cause) {
            super(origin, message, cause);
        }

        public WrongType(ConfigOrigin origin, String message) {
            super(origin, message, null);
        }
    }

    /**
     * Exception indicates that the setting was never set to anything, not even
     * null.
     */
    public static class Missing extends ConfigException {
        private static final long serialVersionUID = 1L;

        public Missing(String path, Throwable cause) {
            super("No configuration setting found for key '" + path + "'",
                    cause);
        }

        public Missing(String path) {
            this(path, null);
        }

        protected Missing(ConfigOrigin origin, String message, Throwable cause) {
            super(origin, message, cause);
        }

        protected Missing(ConfigOrigin origin, String message) {
            this(origin, message, null);
        }
    }

    /**
     * Exception indicates that the setting was treated as missing because it
     * was set to null.
     */
    public static class Null extends Missing {
        private static final long serialVersionUID = 1L;

        private static String makeMessage(String path, String expected) {
            if (expected != null) {
                return "Configuration key '" + path
                        + "' is set to null but expected " + expected;
            } else {
                return "Configuration key '" + path + "' is null";
            }
        }

        public Null(ConfigOrigin origin, String path, String expected,
                    Throwable cause) {
            super(origin, makeMessage(path, expected), cause);
        }

        public Null(ConfigOrigin origin, String path, String expected) {
            this(origin, path, expected, null);
        }
    }

    /**
     * Exception indicating that a value was messed up, for example you may have
     * asked for a duration and the value can't be sensibly parsed as a
     * duration.
     *
     */
    public static class BadValue extends ConfigException {
        private static final long serialVersionUID = 1L;

        public BadValue(ConfigOrigin origin, String path, String message,
                        Throwable cause) {
            super(origin, "Invalid value at '" + path + "': " + message, cause);
        }

        public BadValue(ConfigOrigin origin, String path, String message) {
            this(origin, path, message, null);
        }

        public BadValue(String path, String message, Throwable cause) {
            super("Invalid value at '" + path + "': " + message, cause);
        }

        public BadValue(String path, String message) {
            this(path, message, null);
        }
    }

    /**
     * Exception indicating that a path expression was invalid. Try putting
     * double quotes around path elements that contain "special" characters.
     *
     */
    public static class BadPath extends ConfigException {
        private static final long serialVersionUID = 1L;

        public BadPath(ConfigOrigin origin, String path, String message,
                       Throwable cause) {
            super(origin,
                    path != null ? ("Invalid path '" + path + "': " + message)
                            : message, cause);
        }

        public BadPath(ConfigOrigin origin, String path, String message) {
            this(origin, path, message, null);
        }

        public BadPath(String path, String message, Throwable cause) {
            super(path != null ? ("Invalid path '" + path + "': " + message)
                    : message, cause);
        }

        public BadPath(String path, String message) {
            this(path, message, null);
        }

        public BadPath(ConfigOrigin origin, String message) {
            this(origin, null, message);
        }
    }

    /**
     * Exception indicating that there's a bug in something (possibly the
     * library itself) or the runtime environment is broken. This exception
     * should never be handled; instead, something should be fixed to keep the
     * exception from occurring. This exception can be thrown by any method in
     * the library.
     */
    public static class BugOrBroken extends ConfigException {
        private static final long serialVersionUID = 1L;

        public BugOrBroken(String message, Throwable cause) {
            super(message, cause);
        }

        public BugOrBroken(String message) {
            this(message, null);
        }
    }

    /**
     * Exception indicating that there was an IO error.
     *
     */
    public static class IO extends ConfigException {
        private static final long serialVersionUID = 1L;

        public IO(ConfigOrigin origin, String message, Throwable cause) {
            super(origin, message, cause);
        }

        public IO(ConfigOrigin origin, String message) {
            this(origin, message, null);
        }
    }

    /**
     * Exception indicating that there was a parse error.
     *
     */
    public static class Parse extends ConfigException {
        private static final long serialVersionUID = 1L;

        public Parse(ConfigOrigin origin, String message, Throwable cause) {
            super(origin, message, cause);
        }

        public Parse(ConfigOrigin origin, String message) {
            this(origin, message, null);
        }
    }

    /**
     * Exception indicating that a substitution did not resolve to anything.
     * Thrown by {@link Config#resolve}.
     */
    public static class UnresolvedSubstitution extends Parse {
        private static final long serialVersionUID = 1L;

        public UnresolvedSubstitution(ConfigOrigin origin, String detail, Throwable cause) {
            super(origin, "Could not resolve substitution to a value: " + detail, cause);
        }

        public UnresolvedSubstitution(ConfigOrigin origin, String detail) {
            this(origin, detail, null);
        }
    }

    /**
     * Exception indicating that you tried to use a function that requires
     * substitutions to be resolved, but substitutions have not been resolved
     * (that is, {@link Config#resolve} was not called). This is always a bug in
     * either application code or the library; it's wrong to write a handler for
     * this exception because you should be able to fix the code to avoid it by
     * adding calls to {@link Config#resolve}.
     */
    public static class NotResolved extends BugOrBroken {
        private static final long serialVersionUID = 1L;

        public NotResolved(String message, Throwable cause) {
            super(message, cause);
        }

        public NotResolved(String message) {
            this(message, null);
        }
    }

    /**
     * Information about a problem that occurred in {@link Config#checkValid}. A
     * {@link ConfigException.ValidationFailed} exception thrown from
     * <code>checkValid()</code> includes a list of problems encountered.
     */
    public static class ValidationProblem {

        final private String path;
        final private ConfigOrigin origin;
        final private String problem;

        public ValidationProblem(String path, ConfigOrigin origin, String problem) {
            this.path = path;
            this.origin = origin;
            this.problem = problem;
        }

        /**
         * Returns the config setting causing the problem.
         * @return the path of the problem setting
         */
        public String path() {
            return path;
        }

        /**
         * Returns where the problem occurred (origin may include info on the
         * file, line number, etc.).
         * @return the origin of the problem setting
         */
        public ConfigOrigin origin() {
            return origin;
        }

        /**
         * Returns a description of the problem.
         * @return description of the problem
         */
        public String problem() {
            return problem;
        }

        @Override
        public String toString() {
            return "ValidationProblem(" + path + "," + origin + "," + problem + ")";
        }
    }

    /**
     * Exception indicating that {@link Config#checkValid} found validity
     * problems. The problems are available via the {@link #problems()} method.
     * The <code>getMessage()</code> of this exception is a potentially very
     * long string listing all the problems found.
     */
    public static class ValidationFailed extends ConfigException {
        private static final long serialVersionUID = 1L;

        final private Iterable<ValidationProblem> problems;

        public ValidationFailed(Iterable<ValidationProblem> problems) {
            super(makeMessage(problems), null);
            this.problems = problems;
        }

        public Iterable<ValidationProblem> problems() {
            return problems;
        }

        private static String makeMessage(Iterable<ValidationProblem> problems) {
            StringBuilder sb = new StringBuilder();
            for (ValidationProblem p : problems) {
                sb.append(p.origin().description());
                sb.append(": ");
                sb.append(p.path());
                sb.append(": ");
                sb.append(p.problem());
                sb.append(", ");
            }
            if (sb.length() == 0)
                throw new ConfigException.BugOrBroken(
                        "ValidationFailed must have a non-empty list of problems");
            sb.setLength(sb.length() - 2); // chop comma and space

            return sb.toString();
        }
    }

    /**
     * Some problem with a JavaBean we are trying to initialize.
     * @since 1.3.0
     */
    public static class BadBean extends BugOrBroken {
        private static final long serialVersionUID = 1L;

        public BadBean(String message, Throwable cause) {
            super(message, cause);
        }

        public BadBean(String message) {
            this(message, null);
        }
    }

    /**
     * Exception that doesn't fall into any other category.
     */
    public static class Generic extends ConfigException {
        private static final long serialVersionUID = 1L;

        public Generic(String message, Throwable cause) {
            super(message, cause);
        }

        public Generic(String message) {
            this(message, null);
        }
    }

}
