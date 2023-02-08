package com.hartwig.pipeline.metadata;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZonedDateTime;

import com.hartwig.api.RunApi;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HmfApiStatusUpdateTest {

    @Test
    public void start() {
        // In case of invocating start we want to see status in api change to Processing
        RunApi runApi = mock(RunApi.class);
//        Run run = mock(Run.class)
        Run run = new Run().id(3L);
        HmfApiStatusUpdate victim = new HmfApiStatusUpdate();
        victim.start(runApi, run);
        ArgumentCaptor<UpdateRun> argCap = ArgumentCaptor.forClass(UpdateRun.class);
        verify(runApi).update(eq(3L), argCap.capture());
        assertThat(argCap.getValue().getStatus()).isEqualTo(Status.PROCESSING);
//        ZonedDateTime startTime = argCap.getValue().getStartTime().;
//        2023-02-07T17:10:57.396091
        //        assertThat(argCap.getValue().getStatus()).isEqualTo(Status.PROCESSING);
    }

    @Test
    public void finish() {
    }
}