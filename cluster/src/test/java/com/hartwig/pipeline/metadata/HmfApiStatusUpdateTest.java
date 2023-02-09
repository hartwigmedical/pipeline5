package com.hartwig.pipeline.metadata;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import com.hartwig.api.RunApi;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.assertj.core.data.TemporalUnitLessThanOffset;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HmfApiStatusUpdateTest {

    private static final Long RUN_ID = 3L;
    private RunApi runApi;
    private Run run;
    private ArgumentCaptor<UpdateRun> argCaptor;
    private HmfApiStatusUpdate apiStatusUpdate;

    @Before
    public void setup() {
        runApi = mock(RunApi.class);
        run = new Run().id(RUN_ID);
        argCaptor = ArgumentCaptor.forClass(UpdateRun.class);
        apiStatusUpdate = new HmfApiStatusUpdate(runApi);
    }

    @Test
    public void startSetsApiStatusToProcessing() {
        apiStatusUpdate.start(run.getId());
        verify(runApi).update(eq(RUN_ID), argCaptor.capture());
        assertThat(argCaptor.getValue().getStatus()).isEqualTo(Status.PROCESSING);
        assertThat(LocalDateTime.parse(Objects.requireNonNull(argCaptor.getValue().getStartTime()),
                DateTimeFormatter.ISO_DATE_TIME)).isCloseTo(ZonedDateTime.now(ZoneOffset.UTC).toLocalDateTime(),
                new TemporalUnitLessThanOffset(3, ChronoUnit.SECONDS));
    }

    @Test
    public void successSetsApiStatusToFinished() {
        apiStatusUpdate.finish(run.getId(), PipelineStatus.SUCCESS);
        verify(runApi).update(eq(RUN_ID), argCaptor.capture());
        assertThat(argCaptor.getValue().getStatus()).isEqualTo(Status.FINISHED);
    }

    @Test
    public void qcFailureSetsApiStatusToFailed() {
        apiStatusUpdate.finish(run.getId(), PipelineStatus.QC_FAILED);
        verify(runApi).update(eq(RUN_ID), argCaptor.capture());
        assertThat(argCaptor.getValue().getStatus()).isEqualTo(Status.FAILED);
        assertThat(argCaptor.getValue().getFailure().getType()).isEqualTo(RunFailure.TypeEnum.QCFAILURE);
    }

    @Test
    public void technicalFailureSetsApiStatusToFailed() {
        apiStatusUpdate.finish(run.getId(), PipelineStatus.FAILED);
        verify(runApi).update(eq(RUN_ID), argCaptor.capture());
        assertThat(argCaptor.getValue().getStatus()).isEqualTo(Status.FAILED);
        assertThat(argCaptor.getValue().getFailure().getType()).isEqualTo(RunFailure.TypeEnum.TECHNICALFAILURE);
    }
}