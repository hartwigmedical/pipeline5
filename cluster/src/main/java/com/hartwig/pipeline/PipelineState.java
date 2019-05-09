package com.hartwig.pipeline;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.JobStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PipelineState {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineState.class);
    private final List<StageOutput> stageOutputs = Lists.newArrayList();

    <T extends StageOutput> T addStageOutput(final T stageOutput) {
        stageOutputs.add(stageOutput);
        return stageOutput;
    }

    List<StageOutput> stageOutputs() {
        return stageOutputs;
    }

    boolean shouldProceed() {
        boolean shouldProceed = status() == JobStatus.SUCCESS;
        if (!shouldProceed) {
            LOGGER.warn("Halting pipeline due to required stage failure [{}]", this);
        }
        return shouldProceed;
    }

    JobStatus status() {
        return stageOutputs().stream()
                .map(StageOutput::status)
                .filter(status -> status == JobStatus.FAILED)
                .findFirst()
                .orElse(JobStatus.SUCCESS);
    }

    @Override
    public String toString() {
        return "PipelineState{stageOutputs=[" + stageOutputs.stream()
                .map(stageOutput -> stageOutput.name() + ":" + stageOutput.status() + "\n")
                .collect(Collectors.joining()) + "]}";
    }
}
