/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigIncludeContext;
import io.github.interestinglab.waterdrop.config.ConfigObject;
import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigParseOptions;
import io.github.interestinglab.waterdrop.config.ConfigParseable;
import io.github.interestinglab.waterdrop.config.ConfigSyntax;
import io.github.interestinglab.waterdrop.config.ConfigValue;
import io.github.interestinglab.waterdrop.config.parser.ConfigDocument;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

/**
 * Internal implementation detail, not ABI stable, do not touch.
 * For use only by the {@link io.github.interestinglab.waterdrop.config} package.
 * The point of this class is to avoid "propagating" each
 * overload on "thing which can be parsed" through multiple
 * interfaces. Most interfaces can have just one overload that
 * takes a Parseable. Also it's used as an abstract "resource
 * handle" in the ConfigIncluder interface.
 */
public abstract class Parseable implements ConfigParseable {
    private ConfigIncludeContext includeContext;
    private ConfigParseOptions initialOptions;
    private ConfigOrigin initialOrigin;

    /**
     * Internal implementation detail, not ABI stable, do not touch.
     */
    protected interface Relativizer {
        ConfigParseable relativeTo(String filename);
    }

    private static final ThreadLocal<LinkedList<Parseable>> PARSE_STACK = new ThreadLocal<LinkedList<Parseable>>() {
        @Override
        protected LinkedList<Parseable> initialValue() {
            return new LinkedList<Parseable>();
        }
    };

    private static final int MAX_INCLUDE_DEPTH = 50;

    protected Parseable() {
    }

    private ConfigParseOptions fixupOptions(ConfigParseOptions baseOptions) {
        ConfigSyntax syntax = baseOptions.getSyntax();
        if (syntax == null) {
            syntax = guessSyntax();
        }
        if (syntax == null) {
            syntax = ConfigSyntax.CONF;
        }
        ConfigParseOptions modified = baseOptions.setSyntax(syntax);

        // make sure the app-provided includer falls back to default
        modified = modified.appendIncluder(ConfigImpl.defaultIncluder());
        // make sure the app-provided includer is complete
        modified = modified.setIncluder(SimpleIncluder.makeFull(modified.getIncluder()));

        return modified;
    }

    protected void postConstruct(ConfigParseOptions baseOptions) {
        this.initialOptions = fixupOptions(baseOptions);

        this.includeContext = new SimpleIncludeContext(this);

        if (initialOptions.getOriginDescription() != null) {
            initialOrigin = SimpleConfigOrigin.newSimple(initialOptions.getOriginDescription());
        } else {
            initialOrigin = createOrigin();
        }
    }

    // the general idea is that any work should be in here, not in the
    // constructor, so that exceptions are thrown from the public parse()
    // function and not from the creation of the Parseable.
    // Essentially this is a lazy field. The parser should close the
    // reader when it's done with it.
    // ALSO, IMPORTANT: if the file or URL is not found, this must throw.
    // to support the "allow missing" feature.
    protected abstract Reader reader() throws IOException;

    protected Reader reader(ConfigParseOptions options) throws IOException {
        return reader();
    }

    protected static void trace(String message) {
        if (ConfigImpl.traceLoadsEnable()) {
            ConfigImpl.trace(message);
        }
    }

    ConfigSyntax guessSyntax() {
        return null;
    }

    ConfigSyntax contentType() {
        return null;
    }

    ConfigParseable relativeTo(String filename) {
        // fall back to classpath; we treat the "filename" as absolute
        // (don't add a package name in front),
        // if it starts with "/" then remove the "/", for consistency
        // with ParseableResources.relativeTo
        String resource = filename;
        if (filename.startsWith("/")) {
            resource = filename.substring(1);
        }
        return newResources(resource, options().setOriginDescription(null));
    }

    ConfigIncludeContext includeContext() {
        return includeContext;
    }

    static AbstractConfigObject forceParsedToObject(ConfigValue value) {
        if (value instanceof AbstractConfigObject) {
            return (AbstractConfigObject) value;
        }
        throw new ConfigException.WrongType(value.origin(), "", "object at file root", value
                .valueType().name());
    }

    @Override
    public ConfigObject parse(ConfigParseOptions baseOptions) {

        LinkedList<Parseable> stack = PARSE_STACK.get();
        if (stack.size() >= MAX_INCLUDE_DEPTH) {
            throw new ConfigException.Parse(initialOrigin, "include statements nested more than "
                    + MAX_INCLUDE_DEPTH
                    + " times, you probably have a cycle in your includes. Trace: "
                    + stack);
        }

        stack.addFirst(this);
        try {
            return forceParsedToObject(parseValue(baseOptions));
        } finally {
            stack.removeFirst();
            if (stack.isEmpty()) {
                PARSE_STACK.remove();
            }
        }
    }

    final AbstractConfigValue parseValue(ConfigParseOptions baseOptions) {
        // note that we are NOT using our "initialOptions",
        // but using the ones from the passed-in options. The idea is that
        // callers can get our original options and then parse with different
        // ones if they want.
        ConfigParseOptions options = fixupOptions(baseOptions);

        // passed-in options can override origin
        ConfigOrigin origin;
        if (options.getOriginDescription() != null) {
            origin = SimpleConfigOrigin.newSimple(options.getOriginDescription());
        } else {
            origin = initialOrigin;
        }
        return parseValue(origin, options);
    }

    private final AbstractConfigValue parseValue(ConfigOrigin origin,
                                                 ConfigParseOptions finalOptions) {
        try {
            return rawParseValue(origin, finalOptions);
        } catch (IOException e) {
            if (finalOptions.getAllowMissing()) {
                return SimpleConfigObject.emptyMissing(origin);
            }
            trace("exception loading "
                    + origin.description()
                    + ": " + e.getClass().getName()
                    + ": " + e.getMessage());
            throw new ConfigException.IO(origin,
                    e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    final ConfigDocument parseDocument(ConfigParseOptions baseOptions) {
        // note that we are NOT using our "initialOptions",
        // but using the ones from the passed-in options. The idea is that
        // callers can get our original options and then parse with different
        // ones if they want.
        ConfigParseOptions options = fixupOptions(baseOptions);

        // passed-in options can override origin
        ConfigOrigin origin;
        if (options.getOriginDescription() != null) {
            origin = SimpleConfigOrigin.newSimple(options.getOriginDescription());
        } else {
            origin = initialOrigin;
        }
        return parseDocument(origin, options);
    }

    private final ConfigDocument parseDocument(ConfigOrigin origin,
                                               ConfigParseOptions finalOptions) {
        try {
            return rawParseDocument(origin, finalOptions);
        } catch (IOException e) {
            if (finalOptions.getAllowMissing()) {
                ArrayList<AbstractConfigNode> children = new ArrayList<AbstractConfigNode>();
                children.add(new ConfigNodeObject(new ArrayList<AbstractConfigNode>()));
                return new SimpleConfigDocument(new ConfigNodeRoot(children, origin), finalOptions);
            }
            trace("exception loading "
                    + origin.description()
                    + ": " + e.getClass().getName()
                    + ": " + e.getMessage());
            throw new ConfigException.IO(origin,
                    e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    // this is parseValue without post-processing the IOException or handling
    // options.getAllowMissing()
    protected AbstractConfigValue rawParseValue(ConfigOrigin origin, ConfigParseOptions finalOptions)
            throws IOException {
        Reader reader = reader(finalOptions);

        // after reader() we will have loaded the Content-Type.
        ConfigSyntax contentType = contentType();

        ConfigParseOptions optionsWithContentType;
        if (contentType != null) {
            if (ConfigImpl.traceLoadsEnable() && finalOptions.getSyntax() != null) {
                trace("Overriding syntax "
                        + finalOptions.getSyntax()
                        + " with Content-Type which specified "
                        + contentType);
            }

            optionsWithContentType = finalOptions.setSyntax(contentType);
        } else {
            optionsWithContentType = finalOptions;
        }

        try {
            return rawParseValue(reader, origin, optionsWithContentType);
        } finally {
            reader.close();
        }
    }

    private AbstractConfigValue rawParseValue(Reader reader, ConfigOrigin origin,
                                              ConfigParseOptions finalOptions) throws IOException {
        if (finalOptions.getSyntax() == ConfigSyntax.PROPERTIES) {
            return PropertiesParser.parse(reader, origin);
        }
        Iterator<Token> tokens = Tokenizer.tokenize(origin, reader, finalOptions.getSyntax());
        ConfigNodeRoot document = ConfigDocumentParser.parse(tokens, origin, finalOptions);
        return ConfigParser.parse(document, origin, finalOptions, includeContext());
    }

    // this is parseDocument without post-processing the IOException or handling
    // options.getAllowMissing()
    protected ConfigDocument rawParseDocument(ConfigOrigin origin, ConfigParseOptions finalOptions)
            throws IOException {
        Reader reader = reader(finalOptions);

        // after reader() we will have loaded the Content-Type.
        ConfigSyntax contentType = contentType();

        ConfigParseOptions optionsWithContentType;
        if (contentType != null) {
            if (ConfigImpl.traceLoadsEnable() && finalOptions.getSyntax() != null) {
                trace("Overriding syntax "
                        + finalOptions.getSyntax()
                        + " with Content-Type which specified "
                        + contentType);
            }

            optionsWithContentType = finalOptions.setSyntax(contentType);
        } else {
            optionsWithContentType = finalOptions;
        }

        try {
            return rawParseDocument(reader, origin, optionsWithContentType);
        } finally {
            reader.close();
        }
    }

    private ConfigDocument rawParseDocument(Reader reader, ConfigOrigin origin,
                                            ConfigParseOptions finalOptions) throws IOException {
        Iterator<Token> tokens = Tokenizer.tokenize(origin, reader, finalOptions.getSyntax());
        return new SimpleConfigDocument(ConfigDocumentParser.parse(tokens, origin, finalOptions), finalOptions);
    }

    public ConfigObject parse() {
        return forceParsedToObject(parseValue(options()));
    }

    public ConfigDocument parseConfigDocument() {
        return parseDocument(options());
    }

    AbstractConfigValue parseValue() {
        return parseValue(options());
    }

    @Override
    public final ConfigOrigin origin() {
        return initialOrigin;
    }

    protected abstract ConfigOrigin createOrigin();

    @Override
    public ConfigParseOptions options() {
        return initialOptions;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private static ConfigSyntax syntaxFromExtension(String name) {
        if (name.endsWith(".json")) {
            return ConfigSyntax.JSON;
        } else if (name.endsWith(".conf")) {
            return ConfigSyntax.CONF;
        } else if (name.endsWith(".properties")) {
            return ConfigSyntax.PROPERTIES;
        }
        return null;
    }

    private static Reader readerFromStream(InputStream input) {
        return readerFromStream(input, "UTF-8");
    }

    private static Reader readerFromStream(InputStream input, String encoding) {
        try {
            // well, this is messed up. If we aren't going to close
            // the passed-in InputStream then we have no way to
            // close these readers. So maybe we should not have an
            // InputStream version, only a Reader version.
            Reader reader = new InputStreamReader(input, encoding);
            return new BufferedReader(reader);
        } catch (UnsupportedEncodingException e) {
            throw new ConfigException.BugOrBroken("Java runtime does not support UTF-8", e);
        }
    }

    private static Reader doNotClose(Reader input) {
        return new FilterReader(input) {
            @Override
            public void close() {
                // NOTHING.
            }
        };
    }

    static URL relativeTo(URL url, String filename) {
        // I'm guessing this completely fails on Windows, help wanted
        if (new File(filename).isAbsolute()) {
            return null;
        }

        try {
            URI siblingURI = url.toURI();
            URI relative = new URI(filename);

            // this seems wrong, but it's documented that the last
            // element of the path in siblingURI gets stripped out,
            // so to get something in the same directory as
            // siblingURI we just call resolve().
            URL resolved = siblingURI.resolve(relative).toURL();

            return resolved;
        } catch (MalformedURLException e) {
            return null;
        } catch (URISyntaxException e) {
            return null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    static File relativeTo(File file, String filename) {
        File child = new File(filename);

        if (child.isAbsolute()) {
            return null;
        }

        File parent = file.getParentFile();

        if (parent == null) {
            return null;
        }
        return new File(parent, filename);
    }

    // this is a parseable that doesn't exist and just throws when you try to
    // parse it
    private static final class ParseableNotFound extends Parseable {
        private final String what;
        private final String message;

        ParseableNotFound(String what, String message, ConfigParseOptions options) {
            this.what = what;
            this.message = message;
            postConstruct(options);
        }

        @Override
        protected Reader reader() throws IOException {
            throw new FileNotFoundException(message);
        }

        @Override
        protected ConfigOrigin createOrigin() {
            return SimpleConfigOrigin.newSimple(what);
        }
    }

    public static Parseable newNotFound(String whatNotFound, String message,
                                        ConfigParseOptions options) {
        return new ParseableNotFound(whatNotFound, message, options);
    }

    private static final class ParseableReader extends Parseable {
        private final Reader reader;

        ParseableReader(Reader reader, ConfigParseOptions options) {
            this.reader = reader;
            postConstruct(options);
        }

        @Override
        protected Reader reader() {
            if (ConfigImpl.traceLoadsEnable()) {
                trace("Loading config from reader " + reader);
            }
            return reader;
        }

        @Override
        protected ConfigOrigin createOrigin() {
            return SimpleConfigOrigin.newSimple("Reader");
        }
    }

    // note that we will never close this reader; you have to do it when parsing
    // is complete.
    public static Parseable newReader(Reader reader, ConfigParseOptions options) {

        return new ParseableReader(doNotClose(reader), options);
    }

    private static final class ParseableString extends Parseable {
        private final String input;

        ParseableString(String input, ConfigParseOptions options) {
            this.input = input;
            postConstruct(options);
        }

        @Override
        protected Reader reader() {
            if (ConfigImpl.traceLoadsEnable()) {
                trace("Loading config from a String " + input);
            }
            return new StringReader(input);
        }

        @Override
        protected ConfigOrigin createOrigin() {
            return SimpleConfigOrigin.newSimple("String");
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + input + ")";
        }
    }

    public static Parseable newString(String input, ConfigParseOptions options) {
        return new ParseableString(input, options);
    }

    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String PROPERTIES_CONTENT_TYPE = "text/x-java-properties";
    private static final String HOCON_CONTENT_TYPE = "application/hocon";

    private static class ParseableURL extends Parseable {
        protected final URL input;
        private String contentType = null;

        protected ParseableURL(URL input) {
            this.input = input;
            // does not postConstruct (subclass does it)
        }

        ParseableURL(URL input, ConfigParseOptions options) {
            this(input);
            postConstruct(options);
        }

        @Override
        protected Reader reader() throws IOException {
            throw new ConfigException.BugOrBroken("reader() without options should not be called on ParseableURL");
        }

        private static String acceptContentType(ConfigParseOptions options) {
            if (options.getSyntax() == null) {
                return null;
            }

            switch (options.getSyntax()) {
                case JSON:
                    return JSON_CONTENT_TYPE;
                case CONF:
                    return HOCON_CONTENT_TYPE;
                case PROPERTIES:
                    return PROPERTIES_CONTENT_TYPE;
                default:
            }

            // not sure this is reachable but javac thinks it is
            return null;
        }

        @Override
        protected Reader reader(ConfigParseOptions options) throws IOException {
            try {
                if (ConfigImpl.traceLoadsEnable()) {
                    trace("Loading config from a URL: " + input.toExternalForm());
                }
                URLConnection connection = input.openConnection();

                // allow server to serve multiple types from one URL
                String acceptContent = acceptContentType(options);
                if (acceptContent != null) {
                    connection.setRequestProperty("Accept", acceptContent);
                }

                connection.connect();

                // save content type for later
                contentType = connection.getContentType();
                if (contentType != null) {
                    if (ConfigImpl.traceLoadsEnable()) {
                        trace("URL sets Content-Type: '" + contentType + "'");
                    }
                    contentType = contentType.trim();
                    int semi = contentType.indexOf(';');
                    if (semi >= 0) {
                        contentType = contentType.substring(0, semi);
                    }
                }

                InputStream stream = connection.getInputStream();

                return readerFromStream(stream);
            } catch (FileNotFoundException fnf) {
                // If the resource is not found (HTTP response
                // code 404 or something alike), then it's fine to
                // treat it according to the allowMissing setting
                // and "include" spec.  But if we have something
                // like HTTP 503 it seems to be better to fail
                // early, because this may be a sign of broken
                // environment. Java throws FileNotFoundException
                // if it sees 404 or 410.
                throw fnf;
            } catch (IOException e) {
                throw new ConfigException.BugOrBroken("Cannot load config from URL: " + input.toExternalForm(), e);
            }
        }

        @Override
        ConfigSyntax guessSyntax() {
            return syntaxFromExtension(input.getPath());
        }

        @Override
        ConfigSyntax contentType() {
            if (contentType != null) {
                if (contentType.equals(JSON_CONTENT_TYPE)) {
                    return ConfigSyntax.JSON;
                } else if (contentType.equals(PROPERTIES_CONTENT_TYPE)) {
                    return ConfigSyntax.PROPERTIES;
                } else if (contentType.equals(HOCON_CONTENT_TYPE)) {
                    return ConfigSyntax.CONF;
                }
                if (ConfigImpl.traceLoadsEnable()) {
                    trace("'" + contentType + "' isn't a known content type");
                }
                return null;
            }
            return null;
        }

        @Override
        ConfigParseable relativeTo(String filename) {
            URL url = relativeTo(input, filename);
            if (url == null) {
                return null;
            }
            return newURL(url, options().setOriginDescription(null));
        }

        @Override
        protected ConfigOrigin createOrigin() {
            return SimpleConfigOrigin.newURL(input);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + input.toExternalForm() + ")";
        }
    }

    public static Parseable newURL(URL input, ConfigParseOptions options) {
        // we want file: URLs and files to always behave the same, so switch
        // to a file if it's a file: URL
        if (input.getProtocol().equals("file")) {
            return newFile(ConfigImplUtil.urlToFile(input), options);
        }
        return new ParseableURL(input, options);
    }

    private static final class ParseableFile extends Parseable {
        private final File input;

        ParseableFile(File input, ConfigParseOptions options) {
            this.input = input;
            postConstruct(options);
        }

        @Override
        protected Reader reader() throws IOException {
            if (ConfigImpl.traceLoadsEnable()) {
                trace("Loading config from a file: " + input);
            }
            InputStream stream = new FileInputStream(input);
            return readerFromStream(stream);
        }

        @Override
        ConfigSyntax guessSyntax() {
            return syntaxFromExtension(input.getName());
        }

        @Override
        ConfigParseable relativeTo(String filename) {
            File sibling;
            if ((new File(filename)).isAbsolute()) {
                sibling = new File(filename);
            } else {
                // this may return null
                sibling = relativeTo(input, filename);
            }
            if (sibling == null) {
                return null;
            }
            if (sibling.exists()) {
                trace(sibling + " exists, so loading it as a file");
                return newFile(sibling, options().setOriginDescription(null));
            }
            trace(sibling + " does not exist, so trying it as a classpath resource");
            return super.relativeTo(filename);
        }

        @Override
        protected ConfigOrigin createOrigin() {
            return SimpleConfigOrigin.newFile(input.getPath());
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + input.getPath() + ")";
        }
    }

    public static Parseable newFile(File input, ConfigParseOptions options) {
        return new ParseableFile(input, options);
    }

    private static final class ParseableResourceURL extends ParseableURL {

        private final Relativizer relativizer;
        private final String resource;

        ParseableResourceURL(URL input, ConfigParseOptions options, String resource, Relativizer relativizer) {
            super(input);
            this.relativizer = relativizer;
            this.resource = resource;
            postConstruct(options);
        }

        @Override
        protected ConfigOrigin createOrigin() {
            return SimpleConfigOrigin.newResource(resource, input);
        }

        @Override
        ConfigParseable relativeTo(String filename) {
            return relativizer.relativeTo(filename);
        }
    }

    private static Parseable newResourceURL(URL input, ConfigParseOptions options, String resource, Relativizer relativizer) {
        return new ParseableResourceURL(input, options, resource, relativizer);
    }

    private static final class ParseableResources extends Parseable implements Relativizer {
        private final String resource;

        ParseableResources(String resource, ConfigParseOptions options) {
            this.resource = resource;
            postConstruct(options);
        }

        @Override
        protected Reader reader() throws IOException {
            throw new ConfigException.BugOrBroken("reader() should not be called on resources");
        }

        @Override
        protected AbstractConfigObject rawParseValue(ConfigOrigin origin,
                                                     ConfigParseOptions finalOptions) throws IOException {
            ClassLoader loader = finalOptions.getClassLoader();
            if (loader == null) {
                throw new ConfigException.BugOrBroken(
                        "null class loader; pass in a class loader or use Thread.currentThread().setContextClassLoader()");
            }
            Enumeration<URL> e = loader.getResources(resource);
            if (!e.hasMoreElements()) {
                if (ConfigImpl.traceLoadsEnable()) {
                    trace("Loading config from class loader "
                            + loader + " but there were no resources called "
                            + resource);
                }
                throw new IOException("resource not found on classpath: " + resource);
            }
            AbstractConfigObject merged = SimpleConfigObject.empty(origin);
            while (e.hasMoreElements()) {
                URL url = e.nextElement();

                if (ConfigImpl.traceLoadsEnable()) {
                    trace("Loading config from resource '"
                            + resource + "' URL "
                            + url.toExternalForm()
                            + " from class loader " + loader);
                }

                Parseable element = newResourceURL(url, finalOptions, resource, this);

                AbstractConfigValue v = element.parseValue();

                merged = merged.withFallback(v);
            }

            return merged;
        }

        @Override
        ConfigSyntax guessSyntax() {
            return syntaxFromExtension(resource);
        }

        static String parent(String resource) {
            // the "resource" is not supposed to begin with a "/"
            // because it's supposed to be the raw resource
            // (ClassLoader#getResource), not the
            // resource "syntax" (Class#getResource)
            int i = resource.lastIndexOf('/');
            if (i < 0) {
                return null;
            }
            return resource.substring(0, i);
        }

        @Override
        public ConfigParseable relativeTo(String sibling) {
            if (sibling.startsWith("/")) {
                // if it starts with "/" then don't make it relative to
                // the including resource
                return newResources(sibling.substring(1), options().setOriginDescription(null));
            }
            // here we want to build a new resource name and let
            // the class loader have it, rather than getting the
            // url with getResource() and relativizing to that url.
            // This is needed in case the class loader is going to
            // search a classpath.
            String parent = parent(resource);
            if (parent == null) {
                return newResources(sibling, options().setOriginDescription(null));
            }
            return newResources(parent + "/" + sibling, options()
                    .setOriginDescription(null));
        }

        @Override
        protected ConfigOrigin createOrigin() {
            return SimpleConfigOrigin.newResource(resource);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + resource + ")";
        }
    }

    public static Parseable newResources(Class<?> klass, String resource, ConfigParseOptions options) {
        return newResources(convertResourceName(klass, resource),
                options.setClassLoader(klass.getClassLoader()));
    }

    // this function is supposed to emulate the difference
    // between Class.getResource and ClassLoader.getResource
    // (unfortunately there doesn't seem to be public API for it).
    // We're using it because the Class API is more limited,
    // for example it lacks getResources(). So we want to be able to
    // use ClassLoader directly.
    private static String convertResourceName(Class<?> klass, String resource) {
        if (resource.startsWith("/")) {
            // "absolute" resource, chop the slash
            return resource.substring(1);
        }
        String className = klass.getName();
        int i = className.lastIndexOf('.');
        if (i < 0) {
            // no package
            return resource;
        }
        // need to be relative to the package
        String packageName = className.substring(0, i);
        String packagePath = packageName.replace('.', '/');
        return packagePath + "/" + resource;
    }

    public static Parseable newResources(String resource, ConfigParseOptions options) {
        if (options.getClassLoader() == null) {
            throw new ConfigException.BugOrBroken(
                    "null class loader; pass in a class loader or use Thread.currentThread().setContextClassLoader()");
        }
        return new ParseableResources(resource, options);
    }

    private static final class ParseableProperties extends Parseable {
        private final Properties props;

        ParseableProperties(Properties props, ConfigParseOptions options) {
            this.props = props;
            postConstruct(options);
        }

        @Override
        protected Reader reader() throws IOException {
            throw new ConfigException.BugOrBroken("reader() should not be called on props");
        }

        @Override
        protected AbstractConfigObject rawParseValue(ConfigOrigin origin,
                                                     ConfigParseOptions finalOptions) {
            if (ConfigImpl.traceLoadsEnable()) {
                trace("Loading config from properties " + props);
            }
            return PropertiesParser.fromProperties(origin, props);
        }

        @Override
        ConfigSyntax guessSyntax() {
            return ConfigSyntax.PROPERTIES;
        }

        @Override
        protected ConfigOrigin createOrigin() {
            return SimpleConfigOrigin.newSimple("properties");
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + props.size() + " props)";
        }
    }

    public static Parseable newProperties(Properties properties, ConfigParseOptions options) {
        return new ParseableProperties(properties, options);
    }
}
