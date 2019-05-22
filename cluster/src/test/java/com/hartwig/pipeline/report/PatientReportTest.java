package com.hartwig.pipeline.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.JobStatus;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class PatientReportTest {

    private boolean firstComponentRan;
    private boolean secondComponentRan;
    private PatientReport victim;

    @Before
    public void setUp() throws Exception {
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        victim = new PatientReport(storage, reportBucket);
    }

    @Test
    public void composesAllAddedComponents() {
        victim.add(stageOutput(Lists.newArrayList((s, r, setName) -> firstComponentRan = true,
                (s, r, setName) -> secondComponentRan = true)));
        victim.compose("test");
        assertThat(firstComponentRan).isTrue();
        assertThat(secondComponentRan).isTrue();
    }

    @Test
    public void doesNotFailWhenOneComponentFails() {
        victim.add(stageOutput(Lists.newArrayList((s, r, setName) -> firstComponentRan = true, (s, r, setName) -> {
            throw new RuntimeException();
        }, (s, r, setName) -> secondComponentRan = true)));
        victim.compose("test");
        assertThat(firstComponentRan).isTrue();
        assertThat(secondComponentRan).isTrue();
    }

    @NotNull
    private StageOutput stageOutput(final ArrayList<ReportComponent> components) {
        return new StageOutput() {
            @Override
            public String name() {
                return "test";
            }

            @Override
            public JobStatus status() {
                return JobStatus.SUCCESS;
            }

            @Override
            public List<ReportComponent> reportComponents() {
                return components;
            }
        };
    }
}