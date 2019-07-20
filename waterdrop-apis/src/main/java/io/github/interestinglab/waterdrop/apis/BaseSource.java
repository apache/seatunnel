package io.github.interestinglab.waterdrop.apis;

import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.plugin.Plugin;

public interface BaseSource<Data, Env extends RuntimeEnv> extends Plugin {

    Data getData(Env env);

}
