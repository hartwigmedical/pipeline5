package com.hartwig.pipeline.metadata;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hartwig.api.RunApi;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.jackson.ObjectMappers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiRunStatus {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiRunStatus.class);
    private static final String PIPELINE_SOURCE = "Pipeline";
    private static final String HEALTH_CHECK = "HealthCheck";

    static void start(final RunApi runApi, final Run run) {
        UpdateRun update = new UpdateRun().failure(null).status(Status.PROCESSING).startTime(timestamp());
        try {
            LOGGER.info("Updating API with run status and result [{}]", ObjectMappers.get().writeValueAsString(update));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        runApi.update(run.getId(), update);
    }

    static void finish(final RunApi runApi, final Run run, final PipelineStatus status) {
        runApi.update(run.getId(), statusUpdate(status).endTime(timestamp()));
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
