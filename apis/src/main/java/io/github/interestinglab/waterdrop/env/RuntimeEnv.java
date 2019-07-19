package io.github.interestinglab.waterdrop.env;


import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.plugin.Plugin;

import java.util.List;

/**
 * @author mr_xiong
 * @date 2019-05-28
 * @description
 */
public interface RuntimeEnv<SR extends BaseSource, TF extends BaseTransform, SK extends BaseSink> extends Plugin {

    void start(List<SR> sources, List<TF> transforms, List<SK> sinks);

}
