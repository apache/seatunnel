package io.github.interestinglab.waterdrop.env;

import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.plugin.Plugin;

import java.util.List;

public interface Execution<SR extends BaseSource, TF extends BaseTransform, SK extends BaseSink> extends Plugin<Void> {
    void start(List<SR> sources, List<TF> transforms, List<SK> sinks);
}
