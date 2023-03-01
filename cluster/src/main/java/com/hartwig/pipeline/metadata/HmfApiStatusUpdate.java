package com.hartwig.pipeline.metadata;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.hartwig.api.RunApi;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HmfApiStatusUpdate {

    private final static Logger LOGGER = LoggerFactory.getLogger(HmfApiStatusUpdate.class);
    private static final String PIPELINE_SOURCE = "Pipeline";
    private static final String HEALTH_CHECK = "HealthCheck";
    private final RunApi runApi;

    public HmfApiStatusUpdate(final RunApi runApi) {
        this.runApi = runApi;
    }

    public void start(final Long runId) {
        LOGGER.info("Recording pipeline start status in hmf-api [{}]", Status.PROCESSING);
        runApi.update(runId, new UpdateRun().failure(null).status(Status.PROCESSING).startTime(timestamp()));
    }

    public void finish(final Long runId, final PipelineStatus pipelineStatus) {
        LOGGER.info("Recording pipeline finish status in hmf-api [{}]", pipelineStatus);
        runApi.update(runId, statusUpdate(pipelineStatus).endTime(timestamp()));
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