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

package org.apache.seatunnel.engine.common.loader;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class SeaTunnelChildFirstClassLoader extends SeaTunnelBaseClassLoader {
    private final String[] alwaysParentFirstPatterns;
    private static final String[] DEFAULT_PARENT_FIRST_PATTERNS =
            new String[] {
                "java.",
                "javax.xml",
                "org.xml",
                "org.w3c",
                "org.apache.hadoop",
                "scala.",
                "org.apache.seatunnel.",
                "javax.annotation.",
                "org.slf4j",
                "org.apache.log4j",
                "org.apache.logging",
                "org.apache.commons.logging",
                "com.fasterxml.jackson"
            };

    public SeaTunnelChildFirstClassLoader(List<URL> urls) {
        this(urls, DEFAULT_PARENT_FIRST_PATTERNS);
    }

    public SeaTunnelChildFirstClassLoader(List<URL> urls, String[] alwaysParentFirstPatterns) {
        this(
                urls.toArray(new URL[0]),
                SeaTunnelChildFirstClassLoader.class.getClassLoader(),
                alwaysParentFirstPatterns,
                NOOP_EXCEPTION_HANDLER);
    }

    public SeaTunnelChildFirstClassLoader(List<URL> urls, ClassLoader parent) {
        this(
                urls.toArray(new URL[0]),
                parent,
                DEFAULT_PARENT_FIRST_PATTERNS,
                NOOP_EXCEPTION_HANDLER);
    }

    public SeaTunnelChildFirstClassLoader(
            URL[] urls,
            ClassLoader parent,
            String[] alwaysParentFirstPatterns,
            Consumer<Throwable> classLoadingExceptionHandler) {
        super(urls, parent, classLoadingExceptionHandler);
        this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
    }

    @Override
    protected synchronized Class<?> loadClassWithoutExceptionHandling(String name, boolean resolve)
            throws ClassNotFoundException {
        // First, check if the class has already been loaded
        Class<?> c = findLoadedClass(name);

        if (c == null) {
            // check whether the class should go parent-first
            for (String alwaysParentFirstPattern : alwaysParentFirstPatterns) {
                if (name.startsWith(alwaysParentFirstPattern)) {
                    return super.loadClassWithoutExceptionHandling(name, resolve);
                }
            }

            try {
                // check the URLs
                c = findClass(name);
            } catch (ClassNotFoundException e) {
                // let URLClassLoader do it, which will eventually call the parent
                c = super.loadClassWithoutExceptionHandling(name, resolve);
            }
        }

        if (resolve) {
            resolveClass(c);
        }
        return c;
    }

    @Override
    public URL getResource(String name) {
        // first, try and find it via the URLClassloader
        URL urlClassLoaderResource = findResource(name);
        if (urlClassLoaderResource != null) {
            return urlClassLoaderResource;
        }
        // delegate to super
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        // first get resources from URLClassloader
        Enumeration<URL> urlClassLoaderResources = findResources(name);
        final List<URL> result = new ArrayList<>();

        while (urlClassLoaderResources.hasMoreElements()) {
            result.add(urlClassLoaderResources.nextElement());
        }

        // get parent urls
        Enumeration<URL> parentResources = getParent().getResources(name);
        while (parentResources.hasMoreElements()) {
            result.add(parentResources.nextElement());
        }

        return new Enumeration<URL>() {
            final Iterator<URL> iter = result.iterator();

            public boolean hasMoreElements() {
                return iter.hasNext();
            }

            public URL nextElement() {
                return iter.next();
            }
        };
    }
}
