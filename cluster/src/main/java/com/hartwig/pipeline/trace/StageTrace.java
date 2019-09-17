package com.hartwig.pipeline.trace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StageTrace {

    private static final Logger LOGGER = LoggerFactory.getLogger(StageTrace.class);

    public enum ExecutorType{
        DATAPROC, COMPUTE_ENGINE
    }

    private final String stageName;
    private final String sample;
    private final ExecutorType executorType;

    public StageTrace(final String stageName, final String sample, final ExecutorType executorType) {
        this.stageName = stageName.toUpperCase();
        this.sample = sample;
        this.executorType = executorType;
    }

    public StageTrace start(){
        LOGGER.info("Stage [{}] for [{}] starting and will be run on [{}]", stageName,sample, executorType);
        return this;
    }

    public void stop(){
        LOGGER.info("Stage [{}] complete for [{}]", stageName, sample);
    }
}
