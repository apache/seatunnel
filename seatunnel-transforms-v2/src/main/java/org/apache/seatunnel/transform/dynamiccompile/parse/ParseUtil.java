package org.apache.seatunnel.transform.dynamiccompile.parse;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.concurrent.ConcurrentHashMap;

public abstract class ParseUtil {
    protected static ConcurrentHashMap<String, Class<?>> classCache = new ConcurrentHashMap<>();
    // Abstraction layer: Do not want to serialize and pass the classloader
    protected static String getClassKey(String sourceCode) {
        return new String(DigestUtils.getMd5Digest().digest(sourceCode.getBytes()));
    }
}
