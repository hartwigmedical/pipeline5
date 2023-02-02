package com.hartwig.pipeline.metadata;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.hartwig.api.RunApi;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HmfApiStatusUpdate {

    private final static Logger LOGGER = LoggerFactory.getLogger(HmfApiStatusUpdate.class);
    private static final String PIPELINE_SOURCE = "Pipeline";
    private static final String HEALTH_CHECK = "HealthCheck";
    private final RunApi runApi;
    private final Run run;

    public HmfApiStatusUpdate(final RunApi runApi, final Long runId) {
        this.runApi = runApi;
        this.run = runApi.get(runId);
    }

    public void start() {
        runApi.update(run.getId(), new UpdateRun().failure(null).status(Status.PROCESSING).startTime(timestamp()));
    }

    public void finish(final PipelineState pipelineState) {
        LOGGER.info("Recording pipeline completion with status [{}]", pipelineState.status());
        runApi.update(run.getId(), statusUpdate(pipelineState.status()).endTime(timestamp()));
    }

    private static UpdateRun statusUpdate(final PipelineStatus status) {
        switch (status) {
            case FAILED:
                return new UpdateRun().status(Status.FAILED)
                        .failure(new RunFailure().type(RunFailure.TypeEnum.TECHNICALFAILURE).source(PIPELINE_SOURCE));
            case QC_FAILED:
                return new UpdateRun().status(Status.FAILED)
                        .failure(new RunFailure().type(RunFailure.TypeEnum.QCFAILURE).source(HEALTH_CHECK));
            default:
                return new UpdateRun().status(Status.FINISHED);
        }
    }

    private static String timestamp() {
        return ZonedDateTime.now(ZoneOffset.UTC).toLocalDateTime().format(DateTimeFormatter.ISO_DATE_TIME);
    }
}