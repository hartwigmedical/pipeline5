package com.hartwig.pipeline.reruns;

import static java.util.stream.Collectors.toList;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.sage.SageConfiguration;
import com.hartwig.pipeline.calling.structural.Esvee;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.cram2bam.Cram2Bam;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.reruns.StartingPoint.StartingPoints;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.NamespacesTest;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.chord.Chord;
import com.hartwig.pipeline.tertiary.cider.Cider;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cuppa.Cuppa;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.lilac.Lilac;
import com.hartwig.pipeline.tertiary.linx.LinxGermline;
import com.hartwig.pipeline.tertiary.linx.LinxSomatic;
import com.hartwig.pipeline.tertiary.orange.Orange;
import com.hartwig.pipeline.tertiary.peach.Peach;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.sigs.Sigs;
import com.hartwig.pipeline.tertiary.teal.Teal;
import com.hartwig.pipeline.tertiary.teal.TealBam;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreter;

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
        assertThat(victim.usePersisted(Esvee.NAMESPACE)).isTrue();
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
        assertThat(victim.usePersisted(Esvee.NAMESPACE)).isTrue();
        assertThat(victim.usePersisted(Purple.NAMESPACE)).isTrue();
    }

    @Test
    public void startingPointsAreUpToDate() {
        List<Namespace> endStages = namespacesOf(Sigs.class, Orange.class, Cram2Bam.class, HealthChecker.class, Lilac.class,
                Cuppa.class, Peach.class, LinxSomatic.class, LinxGermline.class, Chord.class, VirusInterpreter.class, Cider.class,
                TealBam.class, Teal.class);
        NamespacesTest.allNamespaces().stream().filter(n -> !endStages.contains(n)).map(Namespace::value).collect(toList()).forEach(n -> {
            boolean referenced = false;
            for (StartingPoints points: StartingPoints.values()) {
                if (new StartingPoint(testArgumentsWithStartingPoint(points.name())).usePersisted(n)) {
                    referenced = true;
                }
            }
            assertThat(referenced).as("Namespace %s is referenced from at least one starting point", n).isTrue();
        });
    }

    public static Arguments testArgumentsWithStartingPoint(final String startingPoint) {
        return Arguments.testDefaultsBuilder().startingPoint(startingPoint).build();
    }

    private static List<Namespace> namespacesOf(Class<? extends Stage>...classes) {
        return Arrays.stream(classes).map(c -> c.getAnnotation(Namespace.class)).collect(toList());
    }
}