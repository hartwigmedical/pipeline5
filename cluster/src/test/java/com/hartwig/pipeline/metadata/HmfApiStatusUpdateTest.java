package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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
import com.hartwig.pdl.OperationalReferences;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.assertj.core.data.TemporalUnitLessThanOffset;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HmfApiStatusUpdateTest {

    private static final Long RUN_ID = 3L;
    private RunApi runApi;
    private Run run;
    private ArgumentCaptor<UpdateRun> argCaptor;
    private PipelineInput pipelineInput;

    @Before
    public void setup() {
        runApi = mock(RunApi.class);
        run = new Run().id(RUN_ID);
        argCaptor = ArgumentCaptor.forClass(UpdateRun.class);
        pipelineInput = PipelineInput.builder()
                .setName(TestInputs.SET)
                .operationalReferences(OperationalReferences.builder().runId(RUN_ID).setId(1L).build())
                .build();
    }

    private HmfApiStatusUpdate setupForNoOp() {
        return HmfApiStatusUpdate.from(Arguments.testDefaults(), runApi, mock(PipelineInput.class));
    }

    private HmfApiStatusUpdate setupForRealUpdate() {
        Arguments arguments = Arguments.testDefaultsBuilder().hmfApiUrl("http://api").build();
        return HmfApiStatusUpdate.from(arguments, runApi, pipelineInput);
    }

    @Test
    public void startSetsApiStatusToProcessingWhenUrlProvided() {
        setupForRealUpdate().start();
        verify(runApi).update(eq(RUN_ID), argCaptor.capture());
        assertThat(argCaptor.getValue().getStatus()).isEqualTo(Status.PROCESSING);
        assertThat(LocalDateTime.parse(Objects.requireNonNull(argCaptor.getValue().getStartTime()),
                DateTimeFormatter.ISO_DATE_TIME)).isCloseTo(ZonedDateTime.now(ZoneOffset.UTC).toLocalDateTime(),
                new TemporalUnitLessThanOffset(3, ChronoUnit.SECONDS));
    }

    @Test
    public void successSetsApiStatusToFinishedWhenApiProvided() {
        setupForRealUpdate().finish(PipelineStatus.SUCCESS);
        verify(runApi).update(eq(RUN_ID), argCaptor.capture());
        assertThat(argCaptor.getValue().getStatus()).isEqualTo(Status.FINISHED);
    }

    @Test
    public void qcFailureSetsApiStatusToFailedWhenApiProvided() {
        setupForRealUpdate().finish(PipelineStatus.QC_FAILED);
        verify(runApi).update(eq(RUN_ID), argCaptor.capture());
        assertThat(argCaptor.getValue().getStatus()).isEqualTo(Status.FAILED);
        assertThat(argCaptor.getValue().getFailure().getType()).isEqualTo(RunFailure.TypeEnum.QCFAILURE);
    }

    @Test
    public void technicalFailureSetsApiStatusToFailedWhenApiProvided() {
        setupForRealUpdate().finish(PipelineStatus.FAILED);
        verify(runApi).update(eq(RUN_ID), argCaptor.capture());
        assertThat(argCaptor.getValue().getStatus()).isEqualTo(Status.FAILED);
        assertThat(argCaptor.getValue().getFailure().getType()).isEqualTo(RunFailure.TypeEnum.TECHNICALFAILURE);
    }

    @Test
    public void startDoesNoApiCallWhenNoUrlProvided() {
        setupForNoOp().start();
        verifyNoMoreInteractions(runApi);
    }

    @Test
    public void finishDoesNoApiCallOnSuccessWhenNoUrlProvided() {
        setupForNoOp().finish(PipelineStatus.SUCCESS);
        verifyNoMoreInteractions(runApi);
    }

    @Test
    public void finishDoesNoApiCallOnFailureWhenNoUrlProvided() {
        setupForNoOp().finish(PipelineStatus.FAILED);
        verifyNoMoreInteractions(runApi);
    }
}