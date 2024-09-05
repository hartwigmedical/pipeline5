package com.hartwig.pipeline.reruns;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.sage.SageConfiguration;
import com.hartwig.pipeline.calling.structural.Esvee;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.lilac.LilacBamSlicer;
import com.hartwig.pipeline.tertiary.pave.PaveGermline;
import com.hartwig.pipeline.tertiary.pave.PaveSomatic;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.virus.VirusBreakend;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StartingPoint {

    private final StartingPoints startingPoint;

    enum StartingPoints {
        BEGINNING(Collections.emptyList()),
        ALIGNMENT_COMPLETE(List.of(Aligner.NAMESPACE, BamMetrics.NAMESPACE, SnpGenotype.NAMESPACE)),
        CRAM_COMPLETE(concat(ALIGNMENT_COMPLETE.namespaces, List.of(CramConversion.NAMESPACE))),
        SKIP_ESVEE(List.of(Aligner.NAMESPACE,
                BamMetrics.NAMESPACE,
                GermlineCaller.NAMESPACE,
                SnpGenotype.NAMESPACE,
                Esvee.NAMESPACE,
                CramConversion.NAMESPACE,
                LilacBamSlicer.NAMESPACE)),
        CALLING_COMPLETE(concat(CRAM_COMPLETE.namespaces,
                List.of(SageConfiguration.SAGE_SOMATIC_NAMESPACE,
                        GermlineCaller.NAMESPACE,
                        Esvee.NAMESPACE,
                        Cobalt.NAMESPACE,
                        Amber.NAMESPACE,
                        SageConfiguration.SAGE_GERMLINE_NAMESPACE,
                        PaveSomatic.NAMESPACE,
                        PaveGermline.NAMESPACE,
                        VirusBreakend.NAMESPACE,
                        LilacBamSlicer.NAMESPACE))),
        ESVEE_COMPLETE(concat(CALLING_COMPLETE.namespaces, List.of(Esvee.NAMESPACE))),
        PURPLE_COMPLETE(concat(ESVEE_COMPLETE.namespaces, List.of(Purple.NAMESPACE))),

        RERUN_532(concat(SKIP_ESVEE.namespaces,
                List.of(Cobalt.NAMESPACE,
                        Amber.NAMESPACE,
                        SageConfiguration.SAGE_GERMLINE_NAMESPACE,
                        SageConfiguration.SAGE_SOMATIC_NAMESPACE,
                        VirusBreakend.NAMESPACE))),

        RERUN_534(concat(SKIP_ESVEE.namespaces,
                List.of(Cobalt.NAMESPACE,
                        Amber.NAMESPACE,
                        SageConfiguration.SAGE_SOMATIC_NAMESPACE,
                        VirusBreakend.NAMESPACE))),

        RESEARCH_AFTER_531_DIAGNOSTIC(concat(ALIGNMENT_COMPLETE.namespaces, List.of(GermlineCaller.NAMESPACE))),
        BWA_COMPLETE(List.of(Aligner.NAMESPACE));

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
