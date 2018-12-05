package com.hartwig.pipeline.metrics;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;

import com.hartwig.pipeline.performance.PerformanceProfile;

import org.junit.Before;
import org.junit.Test;

public class MetricsTimelineTest {

    private Clock clock;
    private Metrics metrics;
    private MetricsTimeline victim;

    @Before
    public void setUp() throws Exception {
        clock = mock(Clock.class);
        metrics = mock(Metrics.class);
        victim = new MetricsTimeline(clock, metrics);
    }

    @Test(expected = IllegalStateException.class)
    public void callingStopBeforeStartThrowsIllegalState() {
        victim.stop(Stage.bam(PerformanceProfile.mini()));
    }

    @Test
    public void recordsTimeSpentOnEachStage() {
        when(clock.millis()).thenReturn(1L);
        Stage bam = Stage.bam(PerformanceProfile.mini());
        Stage sort = Stage.sortAndIndex(PerformanceProfile.mini());
        victim.start(bam);
        when(clock.millis()).thenReturn(10L);
        victim.start(sort);
        when(clock.millis()).thenReturn(101L);
        victim.stop(bam);
        when(clock.millis()).thenReturn(200L);
        verify(metrics).record(eq("BAM"), eq(PerformanceProfile.mini()), eq(100L));
        victim.stop(sort);
        verify(metrics).record(eq("SORT_INDEX"), eq(PerformanceProfile.mini()), eq(190L));
    }
}