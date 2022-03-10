package com.hartwig.pipeline.reruns;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.sage.SageConfiguration;
import com.hartwig.pipeline.calling.structural.gridss.Gridss;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.purple.Purple;

import org.junit.Test;

public class StartingPointTest {

    @Test(expected = IllegalArgumentException.class)
    public void unknownStartingPointRaisesException() {
        new StartingPoint(testArgumentsWithStartingPoint("unknown"));
    }

    @Test
    public void noStartingPointStartsAtBeginning() {
        StartingPoint victim = new StartingPoint(Arguments.testDefaults());
        assertThat(victim.usePersisted(Aligner.NAMESPACE)).isFalse();
    }

    @Test
    public void alignmentCompleteStartingPoint() {
        StartingPoint victim = new StartingPoint(testArgumentsWithStartingPoint("alignment_complete"));
        assertThat(victim.usePersisted(Aligner.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(BamMetrics.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(GermlineCaller.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(Flagstat.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(SnpGenotype.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(CramConversion.NAMESPACE)).isFalse();
    }

    @Test
    public void cramCompleteStartingPoint() {
        StartingPoint victim = new StartingPoint(testArgumentsWithStartingPoint("alignment_complete"));
        assertThat(victim.usePersisted(CramConversion.NAMESPACE)).isFalse();
    }

    @Test
    public void callingCompleteStartingPoint() {
        StartingPoint victim = new StartingPoint(testArgumentsWithStartingPoint("calling_complete"));
        assertThat(victim.usePersisted(Aligner.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(BamMetrics.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(GermlineCaller.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(Amber.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(Cobalt.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(SageConfiguration.SAGE_SOMATIC_NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(Gridss.NAMESPACE)).isTrue();
    }

    @Test
    public void purpleCompleteStartingPoint() {
        StartingPoint victim = new StartingPoint(testArgumentsWithStartingPoint("purple_complete"));
        assertThat(victim.usePersisted(Aligner.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(BamMetrics.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(GermlineCaller.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(Amber.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(Cobalt.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(SageConfiguration.SAGE_SOMATIC_NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(Gridss.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(Purple.NAMESPACE)).isTrue();
    }

    public static Arguments testArgumentsWithStartingPoint(final String startingPoint) {
        return Arguments.testDefaultsBuilder().startingPoint(startingPoint).build();
    }
}