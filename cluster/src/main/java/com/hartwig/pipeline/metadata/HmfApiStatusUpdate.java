package com.hartwig.pipeline.metadata;

import static java.util.Optional.ofNullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.hartwig.api.RunApi;
import com.hartwig.api.SampleApi;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunSet;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleType;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HmfApiStatusUpdate {

    private final static Logger LOGGER = LoggerFactory.getLogger(HmfApiStatusUpdate.class);
    public static final String FAILED = "Failed";
    private final Run run;
    private final RunApi runApi;

    HmfApiStatusUpdate(final Run run, final RunApi runApi) {
        this.runApi = runApi;
        this.run = run;
    }

    public void complete(final PipelineState pipelineState) {
        LOGGER.info("Recording pipeline completion with status [{}]", pipelineState.status());
        try {
            ApiRunStatus.finish(runApi, run, pipelineState.status());
        } catch (Exception e) {
            ApiRunStatus.finish(runApi, run, PipelineStatus.FAILED);
            throw e;
        }
    }

    public void start() {
        ApiRunStatus.start(runApi, run);
    }
}