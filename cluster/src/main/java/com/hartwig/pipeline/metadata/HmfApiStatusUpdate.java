package com.hartwig.pipeline.metadata;

import com.hartwig.api.RunApi;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pdl.OperationalReferences;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pipeline.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public interface HmfApiStatusUpdate {
    static HmfApiStatusUpdate from(final Arguments arguments, final RunApi runApi, final PipelineInput input) {
        if (arguments.hmfApiUrl().isPresent()) {
            return new RealApiStatusUpdate(runApi, input.operationalReferences().map(OperationalReferences::runId).orElseThrow());
        } else {
            return new NoOpStatusUpdate();
        }
    }

    void start();

    void finish(PipelineStatus status);

    class NoOpStatusUpdate implements HmfApiStatusUpdate {
        @Override
        public void start() {
        }

        @Override
        public void finish(final PipelineStatus status) {
        }
    }

    class RealApiStatusUpdate implements HmfApiStatusUpdate {
        private final static Logger LOGGER = LoggerFactory.getLogger(HmfApiStatusUpdate.class);
        private static final String PIPELINE_SOURCE = "Pipeline";
        private static final String HEALTH_CHECK = "HealthCheck";
        private final RunApi runApi;
        private Long runId;

        public RealApiStatusUpdate(final RunApi runApi, final Long runId) {
            this.runApi = runApi;
            this.runId = runId;
        }

        public void start() {
            LOGGER.info("Recording pipeline start status in hmf-api [{}]", Status.PROCESSING);
            runApi.update(runId, new UpdateRun().failure(null).status(Status.PROCESSING).startTime(timestamp()));
        }

        public void finish(final PipelineStatus PipelineStatus) {
            LOGGER.info("Recording pipeline finish status in hmf-api [{}]", PipelineStatus);
            runApi.update(runId, statusUpdate(PipelineStatus).endTime(timestamp()));
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
}