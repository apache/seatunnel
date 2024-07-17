package org.apache.seatunnel.transform.dynamiccompile.parse;

public class JavaClassParse extends AbstractParse {

    @Override
    public Class<?> parseClass(String sourceCode) {
        return JavaClassUtil.parseWithCache(sourceCode);
    }
}
