package com.hartwig.pipeline;

import java.util.concurrent.ThreadFactory;

import org.jetbrains.annotations.NotNull;

public class PipelineThreadFactory implements ThreadFactory{
    @Override
    public Thread newThread(@NotNull final Runnable r) {
        return null;
    }
}
