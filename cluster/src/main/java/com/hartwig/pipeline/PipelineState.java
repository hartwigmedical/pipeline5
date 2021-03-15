package com.hartwig.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.pipeline.execution.PipelineStatus;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineState {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineState.class);
    private final List<StageOutput> stageOutputs;

    public PipelineState() {
        this.stageOutputs = new ArrayList<>();
    }

    private PipelineState(final List<StageOutput> stageOutputs) {
        this.stageOutputs = stageOutputs;
    }

    public <T extends StageOutput> T add(final T stageOutput) {
        stageOutputs.add(stageOutput);
        return stageOutput;
    }

    PipelineState combineWith(PipelineState state) {
        if (state != null) {
            state.stageOutputs().forEach(this::add);
        } else {
            throw new IllegalArgumentException("State from one of the two pipelines was null. Did that pipeline run successfully?");
        }
        return this;
    }

    PipelineState copy() {
        return new PipelineState(new ArrayList<>(this.stageOutputs()));
    }

    public List<StageOutput> stageOutputs() {
        return stageOutputs;
    }

    public boolean shouldProceed() {
        boolean shouldProceed = status() != PipelineStatus.FAILED;
        if (!shouldProceed) {
            LOGGER.warn("Halting pipeline due to required stage failure [{}]", this);
        }
        return shouldProceed;
    }

    public PipelineStatus status() {
        boolean failed = statusStream().anyMatch(status -> status.equals(PipelineStatus.FAILED));
        boolean qcFailed = statusStream().anyMatch(status -> status.equals(PipelineStatus.QC_FAILED));
        return failed ? PipelineStatus.FAILED : qcFailed ? PipelineStatus.QC_FAILED : PipelineStatus.SUCCESS;
    }

    @NotNull
    public Stream<PipelineStatus> statusStream() {
        return stageOutputs().stream().filter(Objects::nonNull).map(StageOutput::status);
    }

    @Override
    public String toString() {
        return "PipelineState{stageOutputs=[" + stageOutputs.stream()
                .map(stageOutput -> stageOutput.name() + ":" + stageOutput.status() + "\n")
                .collect(Collectors.joining()) + "]}";
    }
}
