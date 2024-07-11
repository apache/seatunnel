package org.apache.seatunnel.transform.dynamiccompile.parse;

import org.apache.commons.codec.digest.DigestUtils;

import groovy.lang.GroovyClassLoader;

import java.util.concurrent.ConcurrentHashMap;

public class GroovyUtil {
    private static final GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
    protected static ConcurrentHashMap<String, Class<?>> classCache = new ConcurrentHashMap<>();

    public static Class<?> parseWithCache(String sourceCode) {
        String md5 = new String(DigestUtils.getMd5Digest().digest(sourceCode.getBytes()));
        return classCache.computeIfAbsent(md5, clazz -> groovyClassLoader.parseClass(sourceCode));
    }
}
