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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.function.Consumer;

public abstract class SeatunnelBaseClassLoader extends URLClassLoader {
    protected static final Consumer<Throwable> NOOP_EXCEPTION_HANDLER = classLoadingException -> {};

    private final Consumer<Throwable> classLoadingExceptionHandler;

    protected SeatunnelBaseClassLoader(List<URL> urls) {
        this(urls.toArray(new URL[0]), SeatunnelBaseClassLoader.class.getClassLoader());
    }

    protected SeatunnelBaseClassLoader(URL[] urls, ClassLoader parent) {
        this(urls, parent, NOOP_EXCEPTION_HANDLER);
    }

    protected SeatunnelBaseClassLoader(URL[] urls, ClassLoader parent, Consumer<Throwable> classLoadingExceptionHandler) {
        super(urls, parent);
        this.classLoadingExceptionHandler = classLoadingExceptionHandler;
    }

    @Override
    protected final Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            return loadClassWithoutExceptionHandling(name, resolve);
        } catch (Throwable classLoadingException) {
            classLoadingExceptionHandler.accept(classLoadingException);
            throw classLoadingException;
        }
    }

    protected Class<?> loadClassWithoutExceptionHandling(String name, boolean resolve) throws ClassNotFoundException {
        return super.loadClass(name, resolve);
    }
}
