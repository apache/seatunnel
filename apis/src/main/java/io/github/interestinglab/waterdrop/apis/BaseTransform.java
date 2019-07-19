package io.github.interestinglab.waterdrop.apis;


import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.plugin.Plugin;

public interface BaseTransform<IN, OUT, Env extends RuntimeEnv> extends Plugin {

    OUT process(IN data, Env env);
}
