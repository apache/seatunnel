package org.apache.seatunnel.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.AccessController;
import java.security.AccessControlContext;
import java.security.PrivilegedAction;
import java.util.*;

public final class SeatunnelServiceLoader<S> {

    private static final String PREFIX = "META-INF/provides/";

    // The class or interface representing the service being loaded
    private Class<S> service;

    // The class loader used to locate, load, and instantiate providers
    private ClassLoader loader;

    // The access control context taken when the MapleServiceLoader is created
    private AccessControlContext acc;

    // Cached providers, in instantiation order
    private LinkedHashMap<String, Class<?>> providers = new LinkedHashMap<>();

    // The current lazy-lookup iterator
    private LazyIterator lookupIterator;

    private SeatunnelServiceLoader(Class<S> svc, ClassLoader cl) {
        service = Objects.requireNonNull(svc, "Service interface cannot be null");
        loader = (cl == null) ? ClassLoader.getSystemClassLoader() : cl;
        acc = (System.getSecurityManager() != null) ? AccessController.getContext() : null;
        providers.clear();
        lookupIterator = new LazyIterator(service, loader);
    }

    private static void fail(Class<?> service, String msg, Throwable cause) throws ServiceConfigurationError {
        throw new ServiceConfigurationError(service.getName() + ": " + msg, cause);
    }

    private static void fail(Class<?> service, String msg) throws ServiceConfigurationError {
        throw new ServiceConfigurationError(service.getName() + ": " + msg);
    }

    private static void fail(Class<?> service, URL u, int line, String msg) throws ServiceConfigurationError {
        fail(service, u + ":" + line + ": " + msg);
    }

    private int parseLine(Class<?> service, URL u, BufferedReader r, int lc, List<String[]> names) throws IOException, ServiceConfigurationError {
        String ln = r.readLine();
        if (ln == null) {
            return -1;
        }

        int ci = ln.indexOf('#');
        if (ci >= 0) ln = ln.substring(0, ci);
        ln = ln.trim();

        String[] namePair = ln.split("=");
        String ck = namePair[0];
        String cn = namePair[1];

        if (namePair.length != 2) {
            fail(service, "Provider " + ln + " format error");
        } else if (providers.containsKey(ck)) {
            fail(service, "Provider " + ln + " is duplicate");
        }

        int n = cn.length();
        if (n != 0) {
            if ((cn.indexOf(' ') >= 0) || (cn.indexOf('\t') >= 0))
                fail(service, u, lc, "Illegal configuration-file syntax");
            int cp = cn.codePointAt(0);
            if (!Character.isJavaIdentifierStart(cp))
                fail(service, u, lc, "Illegal provider-class name: " + cn);
            for (int i = Character.charCount(cp); i < n; i += Character.charCount(cp)) {
                cp = cn.codePointAt(i);
                if (!Character.isJavaIdentifierPart(cp) && (cp != '.'))
                    fail(service, u, lc, "Illegal provider-class name: " + cn);
            }
            names.add(namePair);
        }
        return lc + 1;
    }

    private Iterator<String[]> parse(Class<?> service, URL u) throws ServiceConfigurationError {
        InputStream in = null;
        BufferedReader r = null;
        ArrayList<String[]> names = new ArrayList<>();
        try {
            in = u.openStream();
            r = new BufferedReader(new InputStreamReader(in, "utf-8"));
            int lc = 1;
            while ((lc = parseLine(service, u, r, lc, names)) >= 0) ;
        } catch (IOException x) {
            fail(service, "Error reading configuration file", x);
        } finally {
            try {
                if (r != null) r.close();
                if (in != null) in.close();
            } catch (IOException y) {
                fail(service, "Error closing configuration file", y);
            }
        }
        return names.iterator();
    }

    private class LazyIterator {

        Class<S> service;
        ClassLoader loader;
        Enumeration<URL> configs = null;
        Iterator<String[]> pending = null;
        String[] nextNamePair = null;

        private LazyIterator(Class<S> service, ClassLoader loader) {
            this.service = service;
            this.loader = loader;
        }

        private boolean hasNextService() {
            if (nextNamePair != null) {
                return true;
            }
            if (configs == null) {
                try {
                    String fullName = PREFIX + service.getName();
                    if (loader == null)
                        configs = ClassLoader.getSystemResources(fullName);
                    else
                        configs = loader.getResources(fullName);
                } catch (IOException x) {
                    fail(service, "Error locating configuration files", x);
                }
            }
            while ((pending == null) || !pending.hasNext()) {
                if (!configs.hasMoreElements()) {
                    return false;
                }
                pending = parse(service, configs.nextElement());
            }
            nextNamePair = pending.next();
            return true;
        }

        private Class<?> nextService() {
            if (!hasNextService())
                throw new NoSuchElementException();

            String[] namePair = nextNamePair;
            String cn = namePair[1];

            nextNamePair = null;
            Class<?> c = null;
            try {
                c = Class.forName(namePair[1], false, loader);
            } catch (ClassNotFoundException x) {
                fail(service, "Provider " + cn + " not found");
            }
            if (!service.isAssignableFrom(c)) {
                fail(service, "Provider " + cn + " not a subtype");
            } else {
                providers.put(namePair[0], c);
                return c;
            }
            throw new Error();          // This cannot happen
        }

        private boolean hasNext() {
            if (acc == null) {
                return hasNextService();
            } else {
                PrivilegedAction<Boolean> action = () -> hasNextService();
                return AccessController.doPrivileged(action, acc);
            }
        }

        private Class<?> next() {
            if (acc == null) {
                return nextService();
            } else {
                PrivilegedAction<Class<?>> action = () -> nextService();
                return AccessController.doPrivileged(action, acc);
            }
        }

        public void setProviders() {
            while(hasNext()) {
                next();
            }
        }
    }

    public static <S> Map<String, Class<?>> load(Class<S> service, ClassLoader loader) {
        SeatunnelServiceLoader<S> mapleServiceLoader = new SeatunnelServiceLoader<>(service, loader);
        mapleServiceLoader.lookupIterator.setProviders();
        return mapleServiceLoader.providers;
    }

    public static <S> Map<String, Class<?>> load(Class<S> service) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        return load(service, cl);
    }

    public static <S> Map<String, Class<?>> loadInstalled(Class<S> service) {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        ClassLoader prev = null;
        while (cl != null) {
            prev = cl;
            cl = cl.getParent();
        }
        return load(service, prev);
    }

    public String toString() {
        return "java.util.MapleServiceLoader[" + service.getName() + "]";
    }
}