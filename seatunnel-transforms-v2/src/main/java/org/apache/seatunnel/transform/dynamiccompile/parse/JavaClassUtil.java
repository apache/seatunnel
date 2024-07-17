package org.apache.seatunnel.transform.dynamiccompile.parse;

import org.apache.seatunnel.shade.org.codehaus.commons.compiler.CompileException;
import org.apache.seatunnel.shade.org.codehaus.janino.ClassBodyEvaluator;

import java.util.function.Function;

public class JavaClassUtil extends ParseUtil {

    public static Class<?> parseWithCache(String sourceCode) {

        return classCache.computeIfAbsent(
                getClassKey(sourceCode),
                new Function<String, Class<?>>() {
                    @Override
                    public Class<?> apply(String classKey) {
                        try {
                            ClassBodyEvaluator cbe = new ClassBodyEvaluator();
                            cbe.cook(sourceCode);
                            return cbe.getClazz();

                        } catch (CompileException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }
}
