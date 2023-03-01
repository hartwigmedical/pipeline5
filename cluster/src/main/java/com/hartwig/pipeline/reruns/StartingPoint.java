package com.hartwig.pipeline.reruns;

import static com.hartwig.pipeline.calling.structural.gripss.GripssGermline.GRIPSS_GERMLINE_NAMESPACE;
import static com.hartwig.pipeline.calling.structural.gripss.GripssSomatic.GRIPSS_SOMATIC_NAMESPACE;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
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
import com.hartwig.pipeline.tertiary.chord.Chord;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cuppa.Cuppa;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.lilac.Lilac;
import com.hartwig.pipeline.tertiary.lilac.LilacBamSlicer;
import com.hartwig.pipeline.tertiary.linx.LinxGermline;
import com.hartwig.pipeline.tertiary.linx.LinxSomatic;
import com.hartwig.pipeline.tertiary.orange.Orange;
import com.hartwig.pipeline.tertiary.pave.PaveGermline;
import com.hartwig.pipeline.tertiary.pave.PaveSomatic;
import com.hartwig.pipeline.tertiary.peach.Peach;
import com.hartwig.pipeline.tertiary.protect.Protect;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.rose.Rose;
import com.hartwig.pipeline.tertiary.sigs.Sigs;
import com.hartwig.pipeline.tertiary.virus.VirusBreakend;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreter;

import org.jetbrains.annotations.NotNull;

public class StartingPoint {

    private final StartingPoints startingPoint;

    enum StartingPoints {
        BEGINNING(Collections.emptyList()),
        ALIGNMENT_COMPLETE(List.of(Aligner.NAMESPACE, BamMetrics.NAMESPACE, Flagstat.NAMESPACE, SnpGenotype.NAMESPACE)),
        CRAM_COMPLETE(concat(ALIGNMENT_COMPLETE.namespaces, List.of(CramConversion.NAMESPACE))),
        SKIP_GRIDSS(List.of(Aligner.NAMESPACE,
                BamMetrics.NAMESPACE,
                GermlineCaller.NAMESPACE,
                Flagstat.NAMESPACE,
                SnpGenotype.NAMESPACE,
                Gridss.NAMESPACE,
                CramConversion.NAMESPACE,
                LilacBamSlicer.NAMESPACE)),
        CALLING_COMPLETE(concat(CRAM_COMPLETE.namespaces,
                List.of(SageConfiguration.SAGE_SOMATIC_NAMESPACE,
                        GermlineCaller.NAMESPACE,
                        Gridss.NAMESPACE,
                        Cobalt.NAMESPACE,
                        Amber.NAMESPACE,
                        SageConfiguration.SAGE_GERMLINE_NAMESPACE,
                        PaveSomatic.NAMESPACE,
                        PaveGermline.NAMESPACE,
                        VirusBreakend.NAMESPACE,
                        LilacBamSlicer.NAMESPACE))),
        GRIPSS_COMPLETE(concat(CALLING_COMPLETE.namespaces, List.of(GRIPSS_SOMATIC_NAMESPACE, GRIPSS_GERMLINE_NAMESPACE))),
        PURPLE_COMPLETE(concat(GRIPSS_COMPLETE.namespaces, List.of(Purple.NAMESPACE))),
        PROTECT_ONLY(concat(PURPLE_COMPLETE.namespaces,
                List.of(LinxGermline.NAMESPACE,
                        LinxSomatic.NAMESPACE,
                        Chord.NAMESPACE,
                        Lilac.NAMESPACE,
                        Peach.NAMESPACE,
                        Cuppa.NAMESPACE,
                        Orange.NAMESPACE,
                        Sigs.NAMESPACE,
                        Rose.NAMESPACE,
                        VirusInterpreter.NAMESPACE)));

        private final List<String> namespaces;

        StartingPoints(final List<String> namespaces) {
            this.namespaces = namespaces;
        }

        static List<String> concat(final List<String> first, final List<String> second) {
            return ImmutableList.<String>builder().addAll(first).addAll(second).build();
        }
    }

    public StartingPoint(final Arguments arguments) {
        this.startingPoint = arguments.startingPoint().map(String::toUpperCase).map(toEnum()).orElse(StartingPoints.BEGINNING);
    }

    @NotNull
    public static Function<String, StartingPoints> toEnum() {
        return s -> {
            try {
                return StartingPoints.valueOf(s);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("[%s] is not a valid starting point. [%s] are accepted",
                        s,
                        Stream.of(StartingPoints.values()).map(StartingPoints::toString).collect(Collectors.joining(","))));
            }
        };
    }

    public boolean usePersisted(final String namespace) {
        return startingPoint.namespaces.contains(namespace);
    }
}
