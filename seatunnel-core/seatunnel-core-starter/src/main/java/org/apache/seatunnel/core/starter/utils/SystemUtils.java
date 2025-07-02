/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.core.starter.enums.JavaVersion;

import org.apache.commons.lang3.StringUtils;

import java.io.File;

/**
 * Helpers for {@code java.lang.System}.
 *
 * <p>If a system property cannot be read due to security restrictions, the corresponding field in
 * this class will be set to {@code null} and a message will be written to {@code System.err}.
 *
 * <p>#ThreadSafe#
 *
 * @since 1.0
 */
public class SystemUtils {

    /** The prefix String for all Windows OS. */
    private static final String OS_NAME_WINDOWS_PREFIX = "Windows";

    // System property constants
    // -----------------------------------------------------------------------
    // These MUST be declared first. Other constants depend on this.

    /** The System property key for the user home directory. */
    private static final String USER_HOME_KEY = "user.home";

    /** The System property key for the user directory. */
    private static final String USER_DIR_KEY = "user.dir";

    /** The System property key for the Java IO temporary directory. */
    private static final String JAVA_IO_TMPDIR_KEY = "java.io.tmpdir";

    /** The System property key for the Java home directory. */
    private static final String JAVA_HOME_KEY = "java.home";

    /**
     * The {@code awt.toolkit} System Property.
     *
     * <p>Holds a class name, on Windows XP this is {@code sun.awt.windows.WToolkit}.
     *
     * <p><b>On platforms without a GUI, this value is {@code null}.</b>
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.1
     */
    public static final String AWT_TOOLKIT = getSystemProperty("awt.toolkit");

    /**
     * The {@code file.encoding} System Property.
     *
     * <p>File encoding, such as {@code Cp1252}.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.0
     * @since Java 1.2
     */
    public static final String FILE_ENCODING = getSystemProperty("file.encoding");

    /**
     * The {@code file.separator} System Property. The file separator is:
     *
     * <ul>
     *   <li>{@code "/"} on UNIX
     *   <li>{@code "\"} on Windows.
     * </ul>
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @deprecated Use {@link File#separator}, since it is guaranteed to be a string containing a
     *     single character and it does not require a privilege check.
     * @since Java 1.1
     */
    @Deprecated public static final String FILE_SEPARATOR = getSystemProperty("file.separator");

    /**
     * The {@code java.awt.fonts} System Property.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.1
     */
    public static final String JAVA_AWT_FONTS = getSystemProperty("java.awt.fonts");

    /**
     * The {@code java.awt.graphicsenv} System Property.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.1
     */
    public static final String JAVA_AWT_GRAPHICSENV = getSystemProperty("java.awt.graphicsenv");

    /**
     * The {@code java.awt.headless} System Property. The value of this property is the String
     * {@code "true"} or {@code "false"}.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @see #isJavaAwtHeadless()
     * @since 2.1
     * @since Java 1.4
     */
    public static final String JAVA_AWT_HEADLESS = getSystemProperty("java.awt.headless");

    /**
     * The {@code java.awt.printerjob} System Property.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.1
     */
    public static final String JAVA_AWT_PRINTERJOB = getSystemProperty("java.awt.printerjob");

    /**
     * The {@code java.class.path} System Property. Java class path.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String JAVA_CLASS_PATH = getSystemProperty("java.class.path");

    /**
     * The {@code java.class.version} System Property. Java class format version number.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String JAVA_CLASS_VERSION = getSystemProperty("java.class.version");

    /**
     * The {@code java.compiler} System Property. Name of JIT compiler to use. First in JDK version
     * 1.2. Not used in Sun JDKs after 1.2.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2. Not used in Sun versions after 1.2.
     */
    public static final String JAVA_COMPILER = getSystemProperty("java.compiler");

    /**
     * The {@code java.endorsed.dirs} System Property. Path of endorsed directory or directories.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.4
     */
    public static final String JAVA_ENDORSED_DIRS = getSystemProperty("java.endorsed.dirs");

    /**
     * The {@code java.ext.dirs} System Property. Path of extension directory or directories.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.3
     */
    public static final String JAVA_EXT_DIRS = getSystemProperty("java.ext.dirs");

    /**
     * The {@code java.home} System Property. Java installation directory.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String JAVA_HOME = getSystemProperty(JAVA_HOME_KEY);

    /**
     * The {@code java.io.tmpdir} System Property. Default temp file path.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_IO_TMPDIR = getSystemProperty(JAVA_IO_TMPDIR_KEY);

    /**
     * The {@code java.library.path} System Property. List of paths to search when loading
     * libraries.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_LIBRARY_PATH = getSystemProperty("java.library.path");

    /**
     * The {@code java.runtime.name} System Property. Java Runtime Environment name.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.0
     * @since Java 1.3
     */
    public static final String JAVA_RUNTIME_NAME = getSystemProperty("java.runtime.name");

    /**
     * The {@code java.runtime.version} System Property. Java Runtime Environment version.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.0
     * @since Java 1.3
     */
    public static final String JAVA_RUNTIME_VERSION = getSystemProperty("java.runtime.version");

    /**
     * The {@code java.specification.name} System Property. Java Runtime Environment specification
     * name.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_SPECIFICATION_NAME =
            getSystemProperty("java.specification.name");

    /**
     * The {@code java.specification.vendor} System Property. Java Runtime Environment specification
     * vendor.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_SPECIFICATION_VENDOR =
            getSystemProperty("java.specification.vendor");

    /**
     * The {@code java.specification.version} System Property. Java Runtime Environment
     * specification version.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.3
     */
    public static final String JAVA_SPECIFICATION_VERSION =
            getSystemProperty("java.specification.version");

    private static final JavaVersion JAVA_SPECIFICATION_VERSION_AS_ENUM =
            JavaVersion.get(JAVA_SPECIFICATION_VERSION);

    /**
     * The {@code java.util.prefs.PreferencesFactory} System Property. A class name.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.1
     * @since Java 1.4
     */
    public static final String JAVA_UTIL_PREFS_PREFERENCES_FACTORY =
            getSystemProperty("java.util.prefs.PreferencesFactory");

    /**
     * The {@code java.vendor} System Property. Java vendor-specific string.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String JAVA_VENDOR = getSystemProperty("java.vendor");

    /**
     * The {@code java.vendor.url} System Property. Java vendor URL.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String JAVA_VENDOR_URL = getSystemProperty("java.vendor.url");

    /**
     * The {@code java.version} System Property. Java version number.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String JAVA_VERSION = getSystemProperty("java.version");

    /**
     * The {@code java.vm.info} System Property. Java Virtual Machine implementation info.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.0
     * @since Java 1.2
     */
    public static final String JAVA_VM_INFO = getSystemProperty("java.vm.info");

    /**
     * The {@code java.vm.name} System Property. Java Virtual Machine implementation name.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_VM_NAME = getSystemProperty("java.vm.name");

    /**
     * The {@code java.vm.specification.name} System Property. Java Virtual Machine specification
     * name.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_VM_SPECIFICATION_NAME =
            getSystemProperty("java.vm.specification.name");

    /**
     * The {@code java.vm.specification.vendor} System Property. Java Virtual Machine specification
     * vendor.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_VM_SPECIFICATION_VENDOR =
            getSystemProperty("java.vm.specification.vendor");

    /**
     * The {@code java.vm.specification.version} System Property. Java Virtual Machine specification
     * version.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_VM_SPECIFICATION_VERSION =
            getSystemProperty("java.vm.specification.version");

    /**
     * The {@code java.vm.vendor} System Property. Java Virtual Machine implementation vendor.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_VM_VENDOR = getSystemProperty("java.vm.vendor");

    /**
     * The {@code java.vm.version} System Property. Java Virtual Machine implementation version.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.2
     */
    public static final String JAVA_VM_VERSION = getSystemProperty("java.vm.version");

    /**
     * The {@code line.separator} System Property. Line separator (<code>&quot;\n&quot;</code> on
     * UNIX).
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @deprecated Use {@link System#lineSeparator} instead, since it does not require a privilege
     *     check.
     * @since Java 1.1
     */
    @Deprecated public static final String LINE_SEPARATOR = getSystemProperty("line.separator");

    /**
     * The {@code os.arch} System Property. Operating system architecture.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String OS_ARCH = getSystemProperty("os.arch");

    /**
     * The {@code os.name} System Property. Operating system name.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String OS_NAME = getSystemProperty("os.name");

    /**
     * The {@code os.version} System Property. Operating system version.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String OS_VERSION = getSystemProperty("os.version");

    /**
     * The {@code path.separator} System Property. Path separator (<code>&quot;:&quot;</code> on
     * UNIX).
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @deprecated Use {@link File#pathSeparator}, since it is guaranteed to be a string containing
     *     a single character and it does not require a privilege check.
     * @since Java 1.1
     */
    @Deprecated public static final String PATH_SEPARATOR = getSystemProperty("path.separator");

    /**
     * The {@code user.country} or {@code user.region} System Property. User's country code, such as
     * {@code GB}. First in Java version 1.2 as {@code user.region}. Renamed to {@code user.country}
     * in 1.4
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.0
     * @since Java 1.2
     */
    public static final String USER_COUNTRY =
            getSystemProperty("user.country") == null
                    ? getSystemProperty("user.region")
                    : getSystemProperty("user.country");

    /**
     * The {@code user.dir} System Property. User's current working directory.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String USER_DIR = getSystemProperty(USER_DIR_KEY);

    /**
     * The {@code user.home} System Property. User's home directory.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String USER_HOME = getSystemProperty(USER_HOME_KEY);

    /**
     * The {@code user.language} System Property. User's language code, such as {@code "en"}.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.0
     * @since Java 1.2
     */
    public static final String USER_LANGUAGE = getSystemProperty("user.language");

    /**
     * The {@code user.name} System Property. User's account name.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since Java 1.1
     */
    public static final String USER_NAME = getSystemProperty("user.name");

    /**
     * The {@code user.timezone} System Property. For example: {@code "America/Los_Angeles"}.
     *
     * <p>Defaults to {@code null} if the runtime does not have security access to read this
     * property or the property does not exist.
     *
     * <p>This value is initialized when the class is loaded. If {@link
     * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
     * called after this class is loaded, the value will be out of sync with that System property.
     *
     * @since 2.1
     */
    public static final String USER_TIMEZONE = getSystemProperty("user.timezone");

    // Java version checks
    // -----------------------------------------------------------------------
    // These MUST be declared after those above as they depend on the
    // values being set up

    /**
     * Is {@code true} if this is Java version 1.1 (also 1.1.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     */
    public static final boolean IS_JAVA_1_1 = getJavaVersionMatches("1.1");

    /**
     * Is {@code true} if this is Java version 1.2 (also 1.2.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     */
    public static final boolean IS_JAVA_1_2 = getJavaVersionMatches("1.2");

    /**
     * Is {@code true} if this is Java version 1.3 (also 1.3.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     */
    public static final boolean IS_JAVA_1_3 = getJavaVersionMatches("1.3");

    /**
     * Is {@code true} if this is Java version 1.4 (also 1.4.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     */
    public static final boolean IS_JAVA_1_4 = getJavaVersionMatches("1.4");

    /**
     * Is {@code true} if this is Java version 1.5 (also 1.5.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     */
    public static final boolean IS_JAVA_1_5 = getJavaVersionMatches("1.5");

    /**
     * Is {@code true} if this is Java version 1.6 (also 1.6.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     */
    public static final boolean IS_JAVA_1_6 = getJavaVersionMatches("1.6");

    /**
     * Is {@code true} if this is Java version 1.7 (also 1.7.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     *
     * @since 3.0
     */
    public static final boolean IS_JAVA_1_7 = getJavaVersionMatches("1.7");

    /**
     * Is {@code true} if this is Java version 1.8 (also 1.8.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     *
     * @since 3.3.2
     */
    public static final boolean IS_JAVA_1_8 = getJavaVersionMatches("1.8");

    /**
     * Is {@code true} if this is Java version 1.9 (also 1.9.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     *
     * @since 3.4
     * @deprecated As of release 3.5, replaced by {@link #IS_JAVA_9}
     */
    @Deprecated public static final boolean IS_JAVA_1_9 = getJavaVersionMatches("9");

    /**
     * Is {@code true} if this is Java version 9 (also 9.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     *
     * @since 3.5
     */
    public static final boolean IS_JAVA_9 = getJavaVersionMatches("9");

    /**
     * Is {@code true} if this is Java version 10 (also 10.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     *
     * @since 3.7
     */
    public static final boolean IS_JAVA_10 = getJavaVersionMatches("10");

    /**
     * Is {@code true} if this is Java version 11 (also 11.x versions).
     *
     * <p>The field will return {@code false} if {@link #JAVA_VERSION} is {@code null}.
     *
     * @since 3.8
     */
    public static final boolean IS_JAVA_11 = getJavaVersionMatches("11");

    // Operating system checks
    // -----------------------------------------------------------------------
    // These MUST be declared after those above as they depend on the
    // values being set up
    // OS names from http://www.vamphq.com/os.html
    // Selected ones included - please advise dev@commons.apache.org
    // if you want another added or a mistake corrected

    /**
     * Is {@code true} if this is AIX.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_AIX = getOsMatchesName("AIX");

    /**
     * Is {@code true} if this is HP-UX.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_HP_UX = getOsMatchesName("HP-UX");

    /**
     * Is {@code true} if this is IBM OS/400.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.3
     */
    public static final boolean IS_OS_400 = getOsMatchesName("OS/400");

    /**
     * Is {@code true} if this is Irix.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_IRIX = getOsMatchesName("Irix");

    /**
     * Is {@code true} if this is Linux.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_LINUX =
            getOsMatchesName("Linux") || getOsMatchesName("LINUX");

    /**
     * Is {@code true} if this is Mac.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_MAC = getOsMatchesName("Mac");

    /**
     * Is {@code true} if this is Mac.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_MAC_OSX = getOsMatchesName("Mac OS X");

    /**
     * Is {@code true} if this is Mac OS X Cheetah.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_CHEETAH = getOsMatches("Mac OS X", "10.0");

    /**
     * Is {@code true} if this is Mac OS X Puma.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_PUMA = getOsMatches("Mac OS X", "10.1");

    /**
     * Is {@code true} if this is Mac OS X Jaguar.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_JAGUAR = getOsMatches("Mac OS X", "10.2");

    /**
     * Is {@code true} if this is Mac OS X Panther.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_PANTHER = getOsMatches("Mac OS X", "10.3");

    /**
     * Is {@code true} if this is Mac OS X Tiger.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_TIGER = getOsMatches("Mac OS X", "10.4");

    /**
     * Is {@code true} if this is Mac OS X Leopard.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_LEOPARD = getOsMatches("Mac OS X", "10.5");

    /**
     * Is {@code true} if this is Mac OS X Snow Leopard.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_SNOW_LEOPARD = getOsMatches("Mac OS X", "10.6");

    /**
     * Is {@code true} if this is Mac OS X Lion.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_LION = getOsMatches("Mac OS X", "10.7");

    /**
     * Is {@code true} if this is Mac OS X Mountain Lion.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_MOUNTAIN_LION = getOsMatches("Mac OS X", "10.8");

    /**
     * Is {@code true} if this is Mac OS X Mavericks.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_MAVERICKS = getOsMatches("Mac OS X", "10.9");

    /**
     * Is {@code true} if this is Mac OS X Yosemite.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_MAC_OSX_YOSEMITE = getOsMatches("Mac OS X", "10.10");

    /**
     * Is {@code true} if this is Mac OS X El Capitan.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.5
     */
    public static final boolean IS_OS_MAC_OSX_EL_CAPITAN = getOsMatches("Mac OS X", "10.11");

    /**
     * Is {@code true} if this is FreeBSD.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.1
     */
    public static final boolean IS_OS_FREE_BSD = getOsMatchesName("FreeBSD");

    /**
     * Is {@code true} if this is OpenBSD.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.1
     */
    public static final boolean IS_OS_OPEN_BSD = getOsMatchesName("OpenBSD");

    /**
     * Is {@code true} if this is NetBSD.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.1
     */
    public static final boolean IS_OS_NET_BSD = getOsMatchesName("NetBSD");

    /**
     * Is {@code true} if this is OS/2.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_OS2 = getOsMatchesName("OS/2");

    /**
     * Is {@code true} if this is Solaris.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_SOLARIS = getOsMatchesName("Solaris");

    /**
     * Is {@code true} if this is SunOS.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_SUN_OS = getOsMatchesName("SunOS");

    /**
     * Is {@code true} if this is a UNIX like system, as in any of AIX, HP-UX, Irix, Linux, MacOSX,
     * Solaris or SUN OS.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.1
     */
    public static final boolean IS_OS_UNIX =
            IS_OS_AIX
                    || IS_OS_HP_UX
                    || IS_OS_IRIX
                    || IS_OS_LINUX
                    || IS_OS_MAC_OSX
                    || IS_OS_SOLARIS
                    || IS_OS_SUN_OS
                    || IS_OS_FREE_BSD
                    || IS_OS_OPEN_BSD
                    || IS_OS_NET_BSD;

    /**
     * Is {@code true} if this is Windows.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_WINDOWS = getOsMatchesName(OS_NAME_WINDOWS_PREFIX);

    /**
     * Is {@code true} if this is Windows 2000.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_WINDOWS_2000 =
            getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " 2000");

    /**
     * Is {@code true} if this is Windows 2003.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.1
     */
    public static final boolean IS_OS_WINDOWS_2003 =
            getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " 2003");

    /**
     * Is {@code true} if this is Windows Server 2008.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.1
     */
    public static final boolean IS_OS_WINDOWS_2008 =
            getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " Server 2008");

    /**
     * Is {@code true} if this is Windows Server 2012.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.4
     */
    public static final boolean IS_OS_WINDOWS_2012 =
            getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " Server 2012");

    /**
     * Is {@code true} if this is Windows 95.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_WINDOWS_95 = getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " 95");

    /**
     * Is {@code true} if this is Windows 98.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_WINDOWS_98 = getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " 98");

    /**
     * Is {@code true} if this is Windows ME.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_WINDOWS_ME = getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " Me");

    /**
     * Is {@code true} if this is Windows NT.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_WINDOWS_NT = getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " NT");

    /**
     * Is {@code true} if this is Windows XP.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.0
     */
    public static final boolean IS_OS_WINDOWS_XP = getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " XP");

    // -----------------------------------------------------------------------
    /**
     * Is {@code true} if this is Windows Vista.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 2.4
     */
    public static final boolean IS_OS_WINDOWS_VISTA =
            getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " Vista");

    /**
     * Is {@code true} if this is Windows 7.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.0
     */
    public static final boolean IS_OS_WINDOWS_7 = getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " 7");

    /**
     * Is {@code true} if this is Windows 8.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.2
     */
    public static final boolean IS_OS_WINDOWS_8 = getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " 8");

    /**
     * Is {@code true} if this is Windows 10.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.5
     */
    public static final boolean IS_OS_WINDOWS_10 = getOsMatchesName(OS_NAME_WINDOWS_PREFIX + " 10");

    /**
     * Is {@code true} if this is z/OS.
     *
     * <p>The field will return {@code false} if {@code OS_NAME} is {@code null}.
     *
     * @since 3.5
     */
    // Values on a z/OS system I tested (Gary Gregory - 2016-03-12)
    // os.arch = s390x
    // os.encoding = ISO8859_1
    // os.name = z/OS
    // os.version = 02.02.00
    public static final boolean IS_OS_ZOS = getOsMatchesName("z/OS");

    /**
     * Gets the Java home directory as a {@code File}.
     *
     * @return a directory
     * @throws SecurityException if a security manager exists and its {@code checkPropertyAccess}
     *     method doesn't allow access to the specified system property.
     * @see System#getProperty(String)
     * @since 2.1
     */
    public static File getJavaHome() {
        return new File(System.getProperty(JAVA_HOME_KEY));
    }

    /**
     * Gets the host name from an environment variable.
     *
     * <p>If you want to know what the network stack says is the host name, you should use {@code
     * InetAddress.getLocalHost().getHostName()}.
     *
     * @return the host name.
     * @since 3.6
     */
    public static String getHostName() {
        return IS_OS_WINDOWS ? System.getenv("COMPUTERNAME") : System.getenv("HOSTNAME");
    }

    /**
     * Gets the Java IO temporary directory as a {@code File}.
     *
     * @return a directory
     * @throws SecurityException if a security manager exists and its {@code checkPropertyAccess}
     *     method doesn't allow access to the specified system property.
     * @see System#getProperty(String)
     * @since 2.1
     */
    public static File getJavaIoTmpDir() {
        return new File(System.getProperty(JAVA_IO_TMPDIR_KEY));
    }

    /**
     * Decides if the Java version matches.
     *
     * @param versionPrefix the prefix for the java version
     * @return true if matches, or false if not or can't determine
     */
    private static boolean getJavaVersionMatches(final String versionPrefix) {
        return isJavaVersionMatch(JAVA_SPECIFICATION_VERSION, versionPrefix);
    }

    /**
     * Decides if the operating system matches.
     *
     * @param osNamePrefix the prefix for the OS name
     * @param osVersionPrefix the prefix for the version
     * @return true if matches, or false if not or can't determine
     */
    private static boolean getOsMatches(final String osNamePrefix, final String osVersionPrefix) {
        return isOSMatch(OS_NAME, OS_VERSION, osNamePrefix, osVersionPrefix);
    }

    /**
     * Decides if the operating system matches.
     *
     * @param osNamePrefix the prefix for the OS name
     * @return true if matches, or false if not or can't determine
     */
    private static boolean getOsMatchesName(final String osNamePrefix) {
        return isOSNameMatch(OS_NAME, osNamePrefix);
    }

    // -----------------------------------------------------------------------
    /**
     * Gets a System property, defaulting to {@code null} if the property cannot be read.
     *
     * <p>If a {@code SecurityException} is caught, the return value is {@code null} and a message
     * is written to {@code System.err}.
     *
     * @param property the system property name
     * @return the system property value or {@code null} if a security problem occurs
     */
    private static String getSystemProperty(final String property) {
        try {
            return System.getProperty(property);
        } catch (final SecurityException ex) {
            // we are not allowed to look at this property
            // System.err.println("Caught a SecurityException reading the system property '" +
            // property
            // + "'; the SystemUtils property value will default to null.");
            return null;
        }
    }

    /**
     * Gets an environment variable, defaulting to {@code defaultValue} if the variable cannot be
     * read.
     *
     * <p>If a {@code SecurityException} is caught, the return value is {@code defaultValue} and a
     * message is written to {@code System.err}.
     *
     * @param name the environment variable name
     * @param defaultValue the default value
     * @return the environment variable value or {@code defaultValue} if a security problem occurs
     * @since 3.8
     */
    public static String getEnvironmentVariable(final String name, final String defaultValue) {
        try {
            final String value = System.getenv(name);
            return value == null ? defaultValue : value;
        } catch (final SecurityException ex) {
            // we are not allowed to look at this property
            // System.err.println("Caught a SecurityException reading the environment variable '" +
            // name + "'.");
            return defaultValue;
        }
    }

    /**
     * Gets the user directory as a {@code File}.
     *
     * @return a directory
     * @throws SecurityException if a security manager exists and its {@code checkPropertyAccess}
     *     method doesn't allow access to the specified system property.
     * @see System#getProperty(String)
     * @since 2.1
     */
    public static File getUserDir() {
        return new File(System.getProperty(USER_DIR_KEY));
    }

    /**
     * Gets the user home directory as a {@code File}.
     *
     * @return a directory
     * @throws SecurityException if a security manager exists and its {@code checkPropertyAccess}
     *     method doesn't allow access to the specified system property.
     * @see System#getProperty(String)
     * @since 2.1
     */
    public static File getUserHome() {
        return new File(System.getProperty(USER_HOME_KEY));
    }

    /**
     * Returns whether the {@link #JAVA_AWT_HEADLESS} value is {@code true}.
     *
     * @return {@code true} if {@code JAVA_AWT_HEADLESS} is {@code "true"}, {@code false} otherwise.
     * @see #JAVA_AWT_HEADLESS
     * @since 2.1
     * @since Java 1.4
     */
    public static boolean isJavaAwtHeadless() {
        return Boolean.TRUE.toString().equals(JAVA_AWT_HEADLESS);
    }

    /**
     * Is the Java version at least the requested version.
     *
     * <p>Example input:
     *
     * <ul>
     *   <li>{@code 1.2f} to test for Java 1.2
     *   <li>{@code 1.31f} to test for Java 1.3.1
     * </ul>
     *
     * @param requiredVersion the required version, for example 1.31f
     * @return {@code true} if the actual version is equal or greater than the required version
     */
    public static boolean isJavaVersionAtLeast(final JavaVersion requiredVersion) {
        return JAVA_SPECIFICATION_VERSION_AS_ENUM.atLeast(requiredVersion);
    }

    /**
     * Decides if the Java version matches.
     *
     * <p>This method is package private instead of private to support unit test invocation.
     *
     * @param version the actual Java version
     * @param versionPrefix the prefix for the expected Java version
     * @return true if matches, or false if not or can't determine
     */
    static boolean isJavaVersionMatch(final String version, final String versionPrefix) {
        if (version == null) {
            return false;
        }
        return version.startsWith(versionPrefix);
    }

    /**
     * Decides if the operating system matches.
     *
     * <p>This method is package private instead of private to support unit test invocation.
     *
     * @param osName the actual OS name
     * @param osVersion the actual OS version
     * @param osNamePrefix the prefix for the expected OS name
     * @param osVersionPrefix the prefix for the expected OS version
     * @return true if matches, or false if not or can't determine
     */
    static boolean isOSMatch(
            final String osName,
            final String osVersion,
            final String osNamePrefix,
            final String osVersionPrefix) {
        if (osName == null || osVersion == null) {
            return false;
        }
        return isOSNameMatch(osName, osNamePrefix) && isOSVersionMatch(osVersion, osVersionPrefix);
    }

    /**
     * Decides if the operating system matches.
     *
     * <p>This method is package private instead of private to support unit test invocation.
     *
     * @param osName the actual OS name
     * @param osNamePrefix the prefix for the expected OS name
     * @return true if matches, or false if not or can't determine
     */
    static boolean isOSNameMatch(final String osName, final String osNamePrefix) {
        if (osName == null) {
            return false;
        }
        return osName.startsWith(osNamePrefix);
    }

    /**
     * Decides if the operating system version matches.
     *
     * <p>This method is package private instead of private to support unit test invocation.
     *
     * @param osVersion the actual OS version
     * @param osVersionPrefix the prefix for the expected OS version
     * @return true if matches, or false if not or can't determine
     */
    static boolean isOSVersionMatch(final String osVersion, final String osVersionPrefix) {
        if (StringUtils.isEmpty(osVersion)) {
            return false;
        }
        // Compare parts of the version string instead of using String.startsWith(String) because
        // otherwise
        // osVersionPrefix 10.1 would also match osVersion 10.10
        final String[] versionPrefixParts = osVersionPrefix.split("\\.");
        final String[] versionParts = osVersion.split("\\.");
        for (int i = 0; i < Math.min(versionPrefixParts.length, versionParts.length); i++) {
            if (!versionPrefixParts[i].equals(versionParts[i])) {
                return false;
            }
        }
        return true;
    }

    // -----------------------------------------------------------------------
    /**
     * SystemUtils instances should NOT be constructed in standard programming. Instead, the class
     * should be used as {@code SystemUtils.FILE_SEPARATOR}.
     *
     * <p>This constructor is public to permit tools that require a JavaBean instance to operate.
     */
    public SystemUtils() {
        super();
    }
}
